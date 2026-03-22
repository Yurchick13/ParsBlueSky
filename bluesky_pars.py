
"""
BlueSky Parser - load posts from BlueSky into Neo4j graph DB

Graph structure:
    (BlueSky_Account)-[:POSTED]->(BlueSky_Post)
    (BlueSky_Post)-[:MENTIONS]->(BlueSky_Account)
    (BlueSky_Post)-[:TAGS]->(Hashtag)
    (BlueSky_Post)-[:REBLOG_OF]->(BlueSky_Post)
    (BlueSky_Post)-[:REPLY_TO]->(BlueSky_Post)
    (Text)-[:part]->(BlueSky_Post)
    (URI)-[:part]->(BlueSky_Post)
    (Domain)-[:rel]->(URI)
    (Photo|Video|File)-[:part]->(BlueSky_Post)
"""

import datetime
import hashlib
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

from atproto import Client
from atproto_client.exceptions import UnauthorizedError
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError


# =============================================================================
# CONFIG
# =============================================================================


@dataclass
class Config:
    """Application config."""

    # Neo4j
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")

    #ВВЕСТИ СВОИ ДАННЫЕ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "********************")

    # BlueSky ВВЕСТИ СВОИ ДАННЫЕ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    BLUESKY_IDENTIFIER: str = os.getenv("BLUESKY_IDENTIFIER", "********************")
    BLUESKY_PASSWORD: str = os.getenv("BLUESKY_PASSWORD", "********************")

    # Loading limits
    MAX_PAGES: int = 0  # 0 = unlimited
    MAX_POSTS: int = 0  # 0 = unlimited
    MIN_DATE: Optional[datetime.datetime] = None

    # Accounts to parse
    ACCOUNTS: List[str] = None
    PRIOR: int = 1

    # Logging
    LOG_LEVEL: int = logging.INFO
    LOG_FILE: str = "bluesky_pars.log"

    def __post_init__(self):
        if self.ACCOUNTS is None:
            self.ACCOUNTS = []


# =============================================================================
# LOGGER
# =============================================================================


class Logger:
    """Simple logger with typed messages."""

    ICONS = {
        "info": "[i]",
        "success": "[+]",
        "warning": "[!]",
        "error": "[x]",
        "start": "[>]",
        "end": "[<]",
        "process": "[*]",
        "data": "[#]",
    }

    def __init__(self, name: str, log_file: Optional[str] = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_format = logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

    def _log(self, icon_key: str, message: str, level: int = logging.INFO):
        icon = self.ICONS.get(icon_key, "[-]")
        self.logger.log(level, f"{icon} {message}")

    def info(self, message: str):
        self._log("info", message)

    def success(self, message: str):
        self._log("success", message)

    def warning(self, message: str):
        self._log("warning", message, logging.WARNING)

    def error(self, message: str):
        self._log("error", message, logging.ERROR)

    def start(self, message: str):
        self._log("start", f"START: {message}")

    def end(self, message: str):
        self._log("end", f"END: {message}")

    def process(self, message: str):
        self._log("process", message)

    def data(self, message: str):
        self._log("data", message)

    def separator(self, char: str = "=", length: int = 60):
        self.logger.info(char * length)


# =============================================================================
# NEO4J MANAGER
# =============================================================================


class Neo4jManager:
    """Neo4j connection and query manager."""

    def __init__(self, uri: str, user: str, password: str):
        self.uri = uri
        self.user = user
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._verify_connection()

    def _verify_connection(self):
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
        except AuthError as exc:
            raise ConnectionError(
                f"Neo4j auth failed for user '{self.user}' at '{self.uri}'. "
                f"Check NEO4J_USER/NEO4J_PASSWORD. Original error: {exc}"
            ) from exc
        except Exception as exc:
            raise ConnectionError(f"Neo4j connection failed: {exc}") from exc

    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        with self.driver.session() as session:
            return session.run(query, params or {})

    def execute_write(self, query: str, params: Optional[Dict] = None) -> Any:
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return result.consume()

    def get_stats(self) -> Dict[str, int]:
        labels = ["BlueSky_Account", "BlueSky_Post", "Text", "URI", "Domain", "Hashtag", "Photo", "Video", "File"]
        stats = {}
        with self.driver.session() as session:
            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                record = result.single()
                stats[label] = record["count"] if record else 0
        return stats

    def get_accounts_with_prior(self, prior: int = 1) -> List[Dict]:
        query = "MATCH (a:BlueSky_Account) WHERE a.prior = $prior RETURN a.id AS id, a.last_post_uri AS last_post_uri"
        with self.driver.session() as session:
            result = session.run(query, {"prior": prior})
            return [{"id": r["id"], "last_post_uri": r["last_post_uri"] or ""} for r in result]

    def close(self):
        self.driver.close()


# =============================================================================
# BLUESKY API CLIENT
# =============================================================================


class BlueskyClient:
    """Client for BlueSky API."""

    PROFILE_RE = re.compile(r"https?://bsky\.app/profile/([^/?#]+)", re.IGNORECASE)

    def __init__(self, identifier: str, password: str):
        self.client = Client()
        login_id = self.normalize_actor(identifier)
        try:
            self.client.login(login_id, password)
        except UnauthorizedError as exc:
            raise ConnectionError(
                "BlueSky auth failed: invalid identifier or app password. "
                "Use BLUESKY_IDENTIFIER as handle/email and BLUESKY_PASSWORD as App Password "
                "(BlueSky Settings -> App Passwords)."
            ) from exc
        self._verify_connection()

    def _verify_connection(self):
        try:
            self.client.get_profile(actor=self.client.me.did)
        except Exception as exc:
            raise ConnectionError(f"BlueSky API connection failed: {exc}") from exc

    @classmethod
    def normalize_actor(cls, actor: str) -> str:
        value = actor.strip()
        match = cls.PROFILE_RE.search(value)
        if match:
            return match.group(1)
        if value.startswith("@"):
            return value[1:]
        return value

    @staticmethod
    def _to_dict(obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "dict"):
            return obj.dict()
        return dict(obj)

    def get_account(self, actor: str) -> Optional[Dict[str, Any]]:
        try:
            clean_actor = self.normalize_actor(actor)
            profile = self.client.get_profile(actor=clean_actor)
            return self._to_dict(profile)
        except Exception:
            return None

    def get_author_feed(self, actor: str, cursor: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        try:
            clean_actor = self.normalize_actor(actor)
            response = self.client.get_author_feed(actor=clean_actor, cursor=cursor, limit=limit)
            return self._to_dict(response)
        except Exception as exc:
            raise Exception(f"Failed to fetch feed for {actor}: {exc}") from exc


# =============================================================================
# DATA PARSER
# =============================================================================


class DataParser:
    """Helpers for post text/facets/embed parsing."""

    URI_RE = re.compile(r"https?://[^\s)\]}>\"']+")

    @staticmethod
    def parse_iso_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
        if not value:
            return None
        text = value.replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def get_domain(uri: str) -> Optional[str]:
        return urlparse(uri).netloc or None

    @classmethod
    def extract_links_from_text(cls, text: str) -> List[str]:
        return cls.URI_RE.findall(text or "")

    @classmethod
    def parse_facets(cls, facets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        mentions = []
        tags = []
        links = []

        for facet in facets or []:
            features = facet.get("features", [])
            for feature in features:
                ftype = feature.get("$type", "")
                if ftype.endswith("#mention") and feature.get("did"):
                    mentions.append(feature["did"])
                elif ftype.endswith("#tag") and feature.get("tag"):
                    tags.append(feature["tag"])
                elif ftype.endswith("#link") and feature.get("uri"):
                    links.append(feature["uri"])

        return {
            "mentions": list(dict.fromkeys(mentions)),
            "tags": list(dict.fromkeys(tags)),
            "links": list(dict.fromkeys(links)),
        }

    @staticmethod
    def media_id(seed: str) -> str:
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()


# =============================================================================
# GRAPH LOADER
# =============================================================================


class GraphLoader:
    """Loads BlueSky entities into Neo4j."""

    def __init__(self, db: Neo4jManager, logger: Logger):
        self.db = db
        self.log = logger

    def add_account(self, account: Dict[str, Any], prior: int = 1) -> bool:
        created_at = account.get("created_at") or account.get("indexed_at")
        created_at = DataParser.parse_iso_datetime(created_at)
        created_at_iso = (created_at or datetime.datetime.utcnow()).isoformat()

        query = """
        MERGE (a:BlueSky_Account {id: $id})
        ON CREATE SET
            a.dateC = datetime(),
            a.handle = $handle,
            a.display_name = $display_name,
            a.url = $url,
            a.created_at = datetime($created_at),
            a.followers_count = $followers_count,
            a.following_count = $following_count,
            a.posts_count = $posts_count,
            a.prior = $prior,
            a.last_post_uri = ''
        ON MATCH SET
            a.handle = $handle,
            a.display_name = $display_name,
            a.url = $url,
            a.followers_count = $followers_count,
            a.following_count = $following_count,
            a.posts_count = $posts_count,
            a.prior = $prior,
            a.dateM = datetime()
        """
        params = {
            "id": account.get("did"),
            "handle": account.get("handle", ""),
            "display_name": account.get("display_name", ""),
            "url": f"https://bsky.app/profile/{account.get('did')}",
            "created_at": created_at_iso,
            "followers_count": int(account.get("followers_count") or 0),
            "following_count": int(account.get("follows_count") or 0),
            "posts_count": int(account.get("posts_count") or 0),
            "prior": prior,
        }
        result = self.db.execute_write(query, params)
        return result.counters.nodes_created > 0

    def get_account_info(self, account_id: str) -> Optional[Dict[str, Any]]:
        query = """
        MATCH (a:BlueSky_Account {id: $id})
        RETURN a.prior AS prior, a.handle AS handle, a.last_post_uri AS last_post_uri
        """
        with self.db.driver.session() as session:
            rec = session.run(query, {"id": account_id}).single()
            if not rec:
                return None
            return {
                "prior": rec["prior"],
                "handle": rec["handle"],
                "last_post_uri": rec["last_post_uri"] or "",
            }

    def update_account_last_post_uri(self, account_id: str, last_post_uri: str):
        query = """
        MATCH (a:BlueSky_Account {id: $id})
        SET a.last_post_uri = $last_post_uri, a.dateM = datetime()
        """
        with self.db.driver.session() as session:
            session.run(query, {"id": account_id, "last_post_uri": last_post_uri})

    def _merge_account_stub(self, session, did: str):
        query = """
        MERGE (a:BlueSky_Account {id: $did})
        ON CREATE SET a.dateC = datetime(), a.handle = $did, a.display_name = '', a.last_post_uri = ''
        """
        session.run(query, {"did": did})

    def _extract_post_identity(self, post: Dict[str, Any]) -> Dict[str, Any]:
        author = post.get("author", {})
        record = post.get("record", {})
        return {
            "id": post.get("uri"),
            "cid": post.get("cid"),
            "author_id": author.get("did"),
            "author_handle": author.get("handle", ""),
            "author_display_name": author.get("display_name", ""),
            "created_at": record.get("created_at") or post.get("indexed_at"),
            "indexed_at": post.get("indexed_at"),
            "text": record.get("text", ""),
            "reply": record.get("reply", {}),
            "facets": record.get("facets", []),
            "embed": post.get("embed", {}),
            "reply_count": int(post.get("reply_count") or 0),
            "repost_count": int(post.get("repost_count") or 0),
            "like_count": int(post.get("like_count") or 0),
            "quote_count": int(post.get("quote_count") or 0),
        }

    def save_post(self, post: Dict[str, Any], reason: Optional[Dict[str, Any]] = None) -> bool:
        p = self._extract_post_identity(post)
        if not p["id"] or not p["author_id"]:
            return False

        created = DataParser.parse_iso_datetime(p["created_at"]) or datetime.datetime.utcnow()
        indexed = DataParser.parse_iso_datetime(p["indexed_at"]) or created

        facets_data = DataParser.parse_facets(p["facets"])
        links = set(facets_data["links"]) | set(DataParser.extract_links_from_text(p["text"]))

        with self.db.driver.session() as session:
            exists = session.run("MATCH (p:BlueSky_Post {id: $id}) RETURN p.id AS id", {"id": p["id"]}).single()
            is_new = exists is None

            account_merge = """
            MERGE (a:BlueSky_Account {id: $id})
            ON CREATE SET
                a.dateC = datetime(),
                a.handle = $handle,
                a.display_name = $display_name,
                a.url = $url,
                a.last_post_uri = ''
            ON MATCH SET
                a.handle = coalesce(a.handle, $handle),
                a.display_name = coalesce(a.display_name, $display_name),
                a.dateM = datetime()
            """
            session.run(
                account_merge,
                {
                    "id": p["author_id"],
                    "handle": p["author_handle"],
                    "display_name": p["author_display_name"],
                    "url": f"https://bsky.app/profile/{p['author_id']}",
                },
            )

            post_merge = """
            MERGE (p:BlueSky_Post {id: $id})
            ON CREATE SET p.dateC = datetime()
            SET
                p.cid = $cid,
                p.account_id = $account_id,
                p.created_at = datetime($created_at),
                p.indexed_at = datetime($indexed_at),
                p.reply_count = $reply_count,
                p.repost_count = $repost_count,
                p.like_count = $like_count,
                p.quote_count = $quote_count,
                p.dateM = datetime()
            """
            session.run(
                post_merge,
                {
                    "id": p["id"],
                    "cid": p["cid"],
                    "account_id": p["author_id"],
                    "created_at": created.isoformat(),
                    "indexed_at": indexed.isoformat(),
                    "reply_count": p["reply_count"],
                    "repost_count": p["repost_count"],
                    "like_count": p["like_count"],
                    "quote_count": p["quote_count"],
                },
            )

            session.run(
                """
                MATCH (a:BlueSky_Account {id: $account_id})
                MATCH (p:BlueSky_Post {id: $post_id})
                MERGE (a)-[:POSTED]->(p)
                """,
                {"account_id": p["author_id"], "post_id": p["id"]},
            )

            if p["text"].strip():
                session.run(
                    """
                    MERGE (t:Text {descr: $text})
                    ON CREATE SET t.dateC = datetime()
                    WITH t
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (t)-[:part]->(p)
                    """,
                    {"text": p["text"][:8219], "post_id": p["id"]},
                )

            for uri in links:
                session.run(
                    """
                    MERGE (u:URI {value: $uri})
                    ON CREATE SET u.dateC = datetime()
                    WITH u
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (u)-[:part]->(p)
                    """,
                    {"uri": uri, "post_id": p["id"]},
                )
                domain = DataParser.get_domain(uri)
                if domain:
                    session.run(
                        """
                        MERGE (d:Domain {name: $domain})
                        ON CREATE SET d.dateC = datetime()
                        WITH d
                        MATCH (u:URI {value: $uri})
                        MERGE (d)-[:rel]->(u)
                        """,
                        {"domain": domain, "uri": uri},
                    )

            for mention_did in facets_data["mentions"]:
                self._merge_account_stub(session, mention_did)
                session.run(
                    """
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MATCH (a:BlueSky_Account {id: $mention_id})
                    MERGE (p)-[:MENTIONS]->(a)
                    """,
                    {"post_id": p["id"], "mention_id": mention_did},
                )

            for tag in facets_data["tags"]:
                session.run(
                    """
                    MERGE (h:Hashtag {name: $name})
                    ON CREATE SET h.dateC = datetime()
                    WITH h
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (p)-[:TAGS]->(h)
                    """,
                    {"name": tag, "post_id": p["id"]},
                )

            self._save_media(session, p["embed"], p["id"])

            reply_parent = ((p["reply"] or {}).get("parent") or {}).get("uri")
            if reply_parent:
                session.run(
                    """
                    MERGE (parent:BlueSky_Post {id: $parent_id})
                    ON CREATE SET parent.dateC = datetime()
                    WITH parent
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (p)-[:REPLY_TO]->(parent)
                    """,
                    {"post_id": p["id"], "parent_id": reply_parent},
                )

            reason_type = (reason or {}).get("$type", "")
            if reason_type.endswith("#reasonRepost"):
                by = (reason or {}).get("by", {})
                repost_actor = by.get("did")
                repost_time = (reason or {}).get("indexed_at") or indexed.isoformat()
                if repost_actor:
                    repost_id = f"repost://{repost_actor}/{p['id']}"
                    self._merge_account_stub(session, repost_actor)
                    session.run(
                        """
                        MERGE (rp:BlueSky_Post {id: $repost_id})
                        ON CREATE SET rp.dateC = datetime()
                        SET
                            rp.account_id = $actor_id,
                            rp.created_at = datetime($created_at),
                            rp.indexed_at = datetime($created_at),
                            rp.dateM = datetime(),
                            rp.synthetic = true
                        WITH rp
                        MATCH (orig:BlueSky_Post {id: $orig_id})
                        MERGE (rp)-[:REBLOG_OF]->(orig)
                        """,
                        {
                            "repost_id": repost_id,
                            "actor_id": repost_actor,
                            "created_at": repost_time,
                            "orig_id": p["id"],
                        },
                    )
                    session.run(
                        """
                        MATCH (a:BlueSky_Account {id: $actor_id})
                        MATCH (rp:BlueSky_Post {id: $repost_id})
                        MERGE (a)-[:POSTED]->(rp)
                        """,
                        {"actor_id": repost_actor, "repost_id": repost_id},
                    )

            return is_new

    def _save_media(self, session, embed: Dict[str, Any], post_id: str):
        if not embed:
            return

        etype = embed.get("$type", "")

        if etype.endswith("embed.images#view"):
            for image in embed.get("images", []):
                uri = image.get("fullsize") or image.get("thumb") or ""
                if not uri:
                    continue
                media_id = DataParser.media_id(f"img:{uri}")
                session.run(
                    """
                    MERGE (m:Photo {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'image', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": image.get("fullsize") or image.get("thumb"),
                        "preview": image.get("thumb"),
                        "description": image.get("alt", ""),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.video#view"):
            uri = embed.get("playlist") or embed.get("thumbnail") or ""
            if uri:
                media_id = DataParser.media_id(f"video:{uri}")
                session.run(
                    """
                    MERGE (m:Video {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'video', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": embed.get("playlist") or embed.get("thumbnail"),
                        "preview": embed.get("thumbnail"),
                        "description": embed.get("alt", ""),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.external#view"):
            external = embed.get("external", {})
            uri = external.get("uri")
            if uri:
                media_id = DataParser.media_id(f"ext:{uri}")
                session.run(
                    """
                    MERGE (m:File {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'external', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": uri,
                        "preview": external.get("thumb", ""),
                        "description": external.get("title", ""),
                        "post_id": post_id,
                    },
                )


# =============================================================================
# MAIN APP
# =============================================================================


class BlueskyParser:
    """Main app class."""

    def __init__(self, config: Config):
        self.config = config
        self.log = Logger("BlueskyParser", log_file=config.LOG_FILE, level=config.LOG_LEVEL)
        self.db = Neo4jManager(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD)
        self.bluesky = BlueskyClient(config.BLUESKY_IDENTIFIER, config.BLUESKY_PASSWORD)
        self.loader = GraphLoader(self.db, self.log)

    def add_accounts(self, actors: List[str], prior: int = 1) -> tuple:
        self.log.start(f"Add accounts: {actors}")
        added_count = 0
        updated_count = 0

        for actor in actors:
            self.log.process(f"Resolve account {actor}...")
            account = self.bluesky.get_account(actor)
            if account is None:
                self.log.error(f"Account not found: {actor}")
                continue

            self.log.success(f"Found: {account.get('display_name', '')} (@{account.get('handle', '')})")
            is_created = self.loader.add_account(account, prior)
            if is_created:
                added_count += 1
                self.log.success(f"Added account {actor}")
            else:
                updated_count += 1
                self.log.info(f"Updated account {actor}")

        self.log.end(f"Accounts done: added={added_count}, updated={updated_count}")
        return added_count, updated_count

    def fetch_posts_for_account(self, account_id: str) -> int:
        account_info = self.loader.get_account_info(account_id)
        if account_info is None:
            self.log.error(f"Account {account_id} not found in DB")
            return 0

        handle = account_info["handle"]
        last_post_uri = account_info["last_post_uri"]

        limits = []
        if last_post_uri:
            limits.append(f"last_post_uri={last_post_uri}")
        if self.config.MAX_POSTS > 0:
            limits.append(f"max_posts={self.config.MAX_POSTS}")
        if self.config.MAX_PAGES > 0:
            limits.append(f"max_pages={self.config.MAX_PAGES}")
        if self.config.MIN_DATE:
            limits.append(f"min_date={self.config.MIN_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

        limits_text = ", ".join(limits) if limits else "no limits"
        self.log.start(f"Fetch posts for @{handle} ({limits_text})")

        loaded_count = 0
        page_count = 0
        cursor = None
        first_post_uri = None
        stop_reason = None

        while True:
            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"page limit reached ({self.config.MAX_PAGES})"
                break

            try:
                self.log.process(f"Request feed page {page_count + 1}...")
                response = self.bluesky.get_author_feed(actor=account_id, cursor=cursor, limit=100)
                feed_items = response.get("feed", [])
                next_cursor = response.get("cursor")

                if not feed_items:
                    stop_reason = "feed exhausted"
                    self.log.success("No more feed items")
                    break

                self.log.success(f"Received {len(feed_items)} feed items")

            except Exception as exc:
                self.log.error(f"Feed request error: {exc}")
                self.log.info("Sleep 10s before retry...")
                time.sleep(10)
                continue

            should_break = False
            for item in feed_items:
                post = item.get("post", {})
                post_uri = post.get("uri")
                if not post_uri:
                    continue

                if last_post_uri and post_uri == last_post_uri:
                    stop_reason = f"reached last_post_uri={last_post_uri}"
                    self.log.success(stop_reason)
                    should_break = True
                    break

                record = post.get("record", {})
                created_at = DataParser.parse_iso_datetime(record.get("created_at") or post.get("indexed_at"))
                if self.config.MIN_DATE and created_at and created_at < self.config.MIN_DATE:
                    stop_reason = "min_date reached"
                    self.log.success(f"Reached min_date at {created_at.isoformat()}")
                    should_break = True
                    break

                if first_post_uri is None:
                    first_post_uri = post_uri
                    self.log.info(f"First post uri: {first_post_uri}")

                is_new = self.loader.save_post(post, reason=item.get("reason"))
                loaded_count += 1

                if is_new:
                    self.log.data(f"Loaded posts: {loaded_count} (new)")
                else:
                    self.log.info(f"Loaded posts: {loaded_count} (updated)")

                if self.config.MAX_POSTS > 0 and loaded_count >= self.config.MAX_POSTS:
                    stop_reason = f"post limit reached ({self.config.MAX_POSTS})"
                    self.log.success(stop_reason)
                    should_break = True
                    break

            page_count += 1

            if should_break:
                break

            if not next_cursor:
                stop_reason = "no next cursor"
                break

            cursor = next_cursor

        if first_post_uri:
            self.loader.update_account_last_post_uri(account_id, first_post_uri)
            self.log.info(f"Updated last_post_uri={first_post_uri} for {account_id}")

        result = f"Loaded {loaded_count} posts in {page_count} pages"
        if stop_reason:
            result += f" ({stop_reason})"
        self.log.end(result)

        return loaded_count

    def run(self):
        self.log.separator("#")
        self.log.start("SCRIPT START bluesky_pars.py")
        self.log.separator("#")

        start_time = datetime.datetime.now()

        self.log.info(">>> Step 1: Add accounts")
        self.log.info(f"Accounts list: {self.config.ACCOUNTS}")
        self.add_accounts(self.config.ACCOUNTS, prior=self.config.PRIOR)

        self.log.info(">>> Step 2: Load posts")
        self.log.info(
            f"Settings: max_pages={self.config.MAX_PAGES}, max_posts={self.config.MAX_POSTS}, "
            f"min_date={self.config.MIN_DATE}, prior={self.config.PRIOR}"
        )

        accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
        total_accounts = len(accounts)
        self.log.data(f"Accounts found: {total_accounts}")

        total_loaded = 0
        for idx, account in enumerate(accounts, 1):
            self.log.separator("=", 50)
            self.log.start(f"Process account {idx}/{total_accounts}")
            loaded = self.fetch_posts_for_account(account["id"])
            total_loaded += loaded
            self.log.end(f"Account {idx}/{total_accounts} done")

        end_time = datetime.datetime.now()
        stats = self.db.get_stats()

        self.log.separator("#")
        self.log.end("WORK FINISHED")
        self.log.separator("#")

        self.log.info(f"Execution time: {end_time - start_time}")
        self.log.data(f"Total loaded posts: {total_loaded}")
        self.log.separator("=")
        self.log.info("Graph stats:")
        for label, count in stats.items():
            self.log.data(f"  - {label}: {count}")
        self.log.separator("=")

        self.log.info("Dedup check:")
        self.log.success("  - No duplicates (MERGE by id)")
        self.log.success("  - Re-runs update existing posts")

    def close(self):
        self.db.close()


# =============================================================================
# ENTRYPOINT
# =============================================================================


if __name__ == "__main__":
    # Accounts to parse (handle, did, @handle, or bsky profile URL)
    ACCOUNTS_TO_PARSE = [
        "https://bsky.app/profile/jay.bsky.team",
        "https://bsky.app/profile/atproto.com",
        # "@handle.bsky.social",
        # "did:plc:...",
    ]

    # Loading options
    MAX_PAGES = 3
    MAX_POSTS = 0
    MIN_DATE = None
    PRIOR = 1

    # Example:
    # MIN_DATE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

    config = Config(
        ACCOUNTS=ACCOUNTS_TO_PARSE,
        MAX_PAGES=MAX_PAGES,
        MAX_POSTS=MAX_POSTS,
        MIN_DATE=MIN_DATE,
        PRIOR=PRIOR,
    )

    parser = BlueskyParser(config)
    try:
        parser.run()
    finally:
        parser.close()

"""
BlueSky Parser - load posts from BlueSky into Neo4j graph DB

Graph structure:
    (BlueSky_Account)-[:POSTED]->(BlueSky_Post)
    (BlueSky_Post)-[:MENTIONS]->(BlueSky_Account)
    (BlueSky_Post)-[:TAGS]->(Hashtag)
    (BlueSky_Post)-[:REBLOG_OF]->(BlueSky_Post)
    (BlueSky_Post)-[:REPLY_TO]->(BlueSky_Post)
    (Text)-[:part]->(BlueSky_Post)
    (URI)-[:part]->(BlueSky_Post)
    (Domain)-[:rel]->(URI)
    (Photo|Video|File)-[:part]->(BlueSky_Post)
"""

import datetime
import hashlib
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

from atproto import Client
from neo4j import GraphDatabase


# =============================================================================
# CONFIG
# =============================================================================


@dataclass
class Config:
    """Application config."""

    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # BlueSky
    BLUESKY_IDENTIFIER: str = "your_handle.bsky.social"
    BLUESKY_PASSWORD: str = "your-app-password"

    # Loading limits
    MAX_PAGES: int = 0  # 0 = unlimited
    MAX_POSTS: int = 0  # 0 = unlimited
    MIN_DATE: Optional[datetime.datetime] = None

    # Accounts to parse
    ACCOUNTS: List[str] = None
    PRIOR: int = 1

    # Logging
    LOG_LEVEL: int = logging.INFO
    LOG_FILE: str = "bluesky_pars.log"

    def __post_init__(self):
        if self.ACCOUNTS is None:
            self.ACCOUNTS = []


# =============================================================================
# LOGGER
# =============================================================================


class Logger:
    """Simple logger with typed messages."""

    ICONS = {
        "info": "[i]",
        "success": "[+]",
        "warning": "[!]",
        "error": "[x]",
        "start": "[>]",
        "end": "[<]",
        "process": "[*]",
        "data": "[#]",
    }

    def __init__(self, name: str, log_file: Optional[str] = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_format = logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

    def _log(self, icon_key: str, message: str, level: int = logging.INFO):
        icon = self.ICONS.get(icon_key, "[-]")
        self.logger.log(level, f"{icon} {message}")

    def info(self, message: str):
        self._log("info", message)

    def success(self, message: str):
        self._log("success", message)

    def warning(self, message: str):
        self._log("warning", message, logging.WARNING)

    def error(self, message: str):
        self._log("error", message, logging.ERROR)

    def start(self, message: str):
        self._log("start", f"START: {message}")

    def end(self, message: str):
        self._log("end", f"END: {message}")

    def process(self, message: str):
        self._log("process", message)

    def data(self, message: str):
        self._log("data", message)

    def separator(self, char: str = "=", length: int = 60):
        self.logger.info(char * length)


# =============================================================================
# NEO4J MANAGER
# =============================================================================


class Neo4jManager:
    """Neo4j connection and query manager."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._verify_connection()

    def _verify_connection(self):
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
        except Exception as exc:
            raise ConnectionError(f"Neo4j connection failed: {exc}") from exc

    def execute(self, query: str, params: Optional[Dict] = None) -> Any:
        with self.driver.session() as session:
            return session.run(query, params or {})

    def execute_write(self, query: str, params: Optional[Dict] = None) -> Any:
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return result.consume()

    def get_stats(self) -> Dict[str, int]:
        labels = ["BlueSky_Account", "BlueSky_Post", "Text", "URI", "Domain", "Hashtag", "Photo", "Video", "File"]
        stats = {}
        with self.driver.session() as session:
            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                record = result.single()
                stats[label] = record["count"] if record else 0
        return stats

    def get_accounts_with_prior(self, prior: int = 1) -> List[Dict]:
        query = "MATCH (a:BlueSky_Account) WHERE a.prior = $prior RETURN a.id AS id, a.last_post_uri AS last_post_uri"
        with self.driver.session() as session:
            result = session.run(query, {"prior": prior})
            return [{"id": r["id"], "last_post_uri": r["last_post_uri"] or ""} for r in result]

    def close(self):
        self.driver.close()


# =============================================================================
# BLUESKY API CLIENT
# =============================================================================


class BlueskyClient:
    """Client for BlueSky API."""

    PROFILE_RE = re.compile(r"https?://bsky\.app/profile/([^/?#]+)", re.IGNORECASE)

    def __init__(self, identifier: str, password: str):
        self.client = Client()
        self.client.login(identifier, password)
        self._verify_connection()

    def _verify_connection(self):
        try:
            self.client.get_profile(actor=self.client.me.did)
        except Exception as exc:
            raise ConnectionError(f"BlueSky API connection failed: {exc}") from exc

    @classmethod
    def normalize_actor(cls, actor: str) -> str:
        value = actor.strip()
        match = cls.PROFILE_RE.search(value)
        if match:
            return match.group(1)
        if value.startswith("@"):
            return value[1:]
        return value

    @staticmethod
    def _to_dict(obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "dict"):
            return obj.dict()
        return dict(obj)

    def get_account(self, actor: str) -> Optional[Dict[str, Any]]:
        try:
            clean_actor = self.normalize_actor(actor)
            profile = self.client.get_profile(actor=clean_actor)
            return self._to_dict(profile)
        except Exception:
            return None

    def get_author_feed(self, actor: str, cursor: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        try:
            clean_actor = self.normalize_actor(actor)
            response = self.client.get_author_feed(actor=clean_actor, cursor=cursor, limit=limit)
            return self._to_dict(response)
        except Exception as exc:
            raise Exception(f"Failed to fetch feed for {actor}: {exc}") from exc


# =============================================================================
# DATA PARSER
# =============================================================================


class DataParser:
    """Helpers for post text/facets/embed parsing."""

    URI_RE = re.compile(r"https?://[^\s)\]}>\"']+")

    @staticmethod
    def parse_iso_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
        if not value:
            return None
        text = value.replace("Z", "+00:00")
        try:
            return datetime.datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def get_domain(uri: str) -> Optional[str]:
        return urlparse(uri).netloc or None

    @classmethod
    def extract_links_from_text(cls, text: str) -> List[str]:
        return cls.URI_RE.findall(text or "")

    @classmethod
    def parse_facets(cls, facets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        mentions = []
        tags = []
        links = []

        for facet in facets or []:
            features = facet.get("features", [])
            for feature in features:
                ftype = feature.get("$type", "")
                if ftype.endswith("#mention") and feature.get("did"):
                    mentions.append(feature["did"])
                elif ftype.endswith("#tag") and feature.get("tag"):
                    tags.append(feature["tag"])
                elif ftype.endswith("#link") and feature.get("uri"):
                    links.append(feature["uri"])

        return {
            "mentions": list(dict.fromkeys(mentions)),
            "tags": list(dict.fromkeys(tags)),
            "links": list(dict.fromkeys(links)),
        }

    @staticmethod
    def media_id(seed: str) -> str:
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()


# =============================================================================
# GRAPH LOADER
# =============================================================================


class GraphLoader:
    """Loads BlueSky entities into Neo4j."""

    def __init__(self, db: Neo4jManager, logger: Logger):
        self.db = db
        self.log = logger

    def add_account(self, account: Dict[str, Any], prior: int = 1) -> bool:
        created_at = account.get("created_at") or account.get("indexed_at")
        created_at = DataParser.parse_iso_datetime(created_at)
        created_at_iso = (created_at or datetime.datetime.utcnow()).isoformat()

        query = """
        MERGE (a:BlueSky_Account {id: $id})
        ON CREATE SET
            a.dateC = datetime(),
            a.handle = $handle,
            a.display_name = $display_name,
            a.url = $url,
            a.created_at = datetime($created_at),
            a.followers_count = $followers_count,
            a.following_count = $following_count,
            a.posts_count = $posts_count,
            a.prior = $prior,
            a.last_post_uri = ''
        ON MATCH SET
            a.handle = $handle,
            a.display_name = $display_name,
            a.url = $url,
            a.followers_count = $followers_count,
            a.following_count = $following_count,
            a.posts_count = $posts_count,
            a.prior = $prior,
            a.dateM = datetime()
        """
        params = {
            "id": account.get("did"),
            "handle": account.get("handle", ""),
            "display_name": account.get("display_name", ""),
            "url": f"https://bsky.app/profile/{account.get('did')}",
            "created_at": created_at_iso,
            "followers_count": int(account.get("followers_count") or 0),
            "following_count": int(account.get("follows_count") or 0),
            "posts_count": int(account.get("posts_count") or 0),
            "prior": prior,
        }
        result = self.db.execute_write(query, params)
        return result.counters.nodes_created > 0

    def get_account_info(self, account_id: str) -> Optional[Dict[str, Any]]:
        query = """
        MATCH (a:BlueSky_Account {id: $id})
        RETURN a.prior AS prior, a.handle AS handle, a.last_post_uri AS last_post_uri
        """
        with self.db.driver.session() as session:
            rec = session.run(query, {"id": account_id}).single()
            if not rec:
                return None
            return {
                "prior": rec["prior"],
                "handle": rec["handle"],
                "last_post_uri": rec["last_post_uri"] or "",
            }

    def update_account_last_post_uri(self, account_id: str, last_post_uri: str):
        query = """
        MATCH (a:BlueSky_Account {id: $id})
        SET a.last_post_uri = $last_post_uri, a.dateM = datetime()
        """
        with self.db.driver.session() as session:
            session.run(query, {"id": account_id, "last_post_uri": last_post_uri})

    def _merge_account_stub(self, session, did: str):
        query = """
        MERGE (a:BlueSky_Account {id: $did})
        ON CREATE SET a.dateC = datetime(), a.handle = $did, a.display_name = '', a.last_post_uri = ''
        """
        session.run(query, {"did": did})

    def _extract_post_identity(self, post: Dict[str, Any]) -> Dict[str, Any]:
        author = post.get("author", {})
        record = post.get("record", {})
        return {
            "id": post.get("uri"),
            "cid": post.get("cid"),
            "author_id": author.get("did"),
            "author_handle": author.get("handle", ""),
            "author_display_name": author.get("display_name", ""),
            "created_at": record.get("created_at") or post.get("indexed_at"),
            "indexed_at": post.get("indexed_at"),
            "text": record.get("text", ""),
            "reply": record.get("reply", {}),
            "facets": record.get("facets", []),
            "embed": post.get("embed", {}),
            "reply_count": int(post.get("reply_count") or 0),
            "repost_count": int(post.get("repost_count") or 0),
            "like_count": int(post.get("like_count") or 0),
            "quote_count": int(post.get("quote_count") or 0),
        }

    def save_post(self, post: Dict[str, Any], reason: Optional[Dict[str, Any]] = None) -> bool:
        p = self._extract_post_identity(post)
        if not p["id"] or not p["author_id"]:
            return False

        created = DataParser.parse_iso_datetime(p["created_at"]) or datetime.datetime.utcnow()
        indexed = DataParser.parse_iso_datetime(p["indexed_at"]) or created

        facets_data = DataParser.parse_facets(p["facets"])
        links = set(facets_data["links"]) | set(DataParser.extract_links_from_text(p["text"]))

        with self.db.driver.session() as session:
            exists = session.run("MATCH (p:BlueSky_Post {id: $id}) RETURN p.id AS id", {"id": p["id"]}).single()
            is_new = exists is None

            account_merge = """
            MERGE (a:BlueSky_Account {id: $id})
            ON CREATE SET
                a.dateC = datetime(),
                a.handle = $handle,
                a.display_name = $display_name,
                a.url = $url,
                a.last_post_uri = ''
            ON MATCH SET
                a.handle = coalesce(a.handle, $handle),
                a.display_name = coalesce(a.display_name, $display_name),
                a.dateM = datetime()
            """
            session.run(
                account_merge,
                {
                    "id": p["author_id"],
                    "handle": p["author_handle"],
                    "display_name": p["author_display_name"],
                    "url": f"https://bsky.app/profile/{p['author_id']}",
                },
            )

            post_merge = """
            MERGE (p:BlueSky_Post {id: $id})
            ON CREATE SET p.dateC = datetime()
            SET
                p.cid = $cid,
                p.account_id = $account_id,
                p.created_at = datetime($created_at),
                p.indexed_at = datetime($indexed_at),
                p.reply_count = $reply_count,
                p.repost_count = $repost_count,
                p.like_count = $like_count,
                p.quote_count = $quote_count,
                p.dateM = datetime()
            """
            session.run(
                post_merge,
                {
                    "id": p["id"],
                    "cid": p["cid"],
                    "account_id": p["author_id"],
                    "created_at": created.isoformat(),
                    "indexed_at": indexed.isoformat(),
                    "reply_count": p["reply_count"],
                    "repost_count": p["repost_count"],
                    "like_count": p["like_count"],
                    "quote_count": p["quote_count"],
                },
            )

            session.run(
                """
                MATCH (a:BlueSky_Account {id: $account_id})
                MATCH (p:BlueSky_Post {id: $post_id})
                MERGE (a)-[:POSTED]->(p)
                """,
                {"account_id": p["author_id"], "post_id": p["id"]},
            )

            if p["text"].strip():
                session.run(
                    """
                    MERGE (t:Text {descr: $text})
                    ON CREATE SET t.dateC = datetime()
                    WITH t
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (t)-[:part]->(p)
                    """,
                    {"text": p["text"][:8219], "post_id": p["id"]},
                )

            for uri in links:
                session.run(
                    """
                    MERGE (u:URI {value: $uri})
                    ON CREATE SET u.dateC = datetime()
                    WITH u
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (u)-[:part]->(p)
                    """,
                    {"uri": uri, "post_id": p["id"]},
                )
                domain = DataParser.get_domain(uri)
                if domain:
                    session.run(
                        """
                        MERGE (d:Domain {name: $domain})
                        ON CREATE SET d.dateC = datetime()
                        WITH d
                        MATCH (u:URI {value: $uri})
                        MERGE (d)-[:rel]->(u)
                        """,
                        {"domain": domain, "uri": uri},
                    )

            for mention_did in facets_data["mentions"]:
                self._merge_account_stub(session, mention_did)
                session.run(
                    """
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MATCH (a:BlueSky_Account {id: $mention_id})
                    MERGE (p)-[:MENTIONS]->(a)
                    """,
                    {"post_id": p["id"], "mention_id": mention_did},
                )

            for tag in facets_data["tags"]:
                session.run(
                    """
                    MERGE (h:Hashtag {name: $name})
                    ON CREATE SET h.dateC = datetime()
                    WITH h
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (p)-[:TAGS]->(h)
                    """,
                    {"name": tag, "post_id": p["id"]},
                )

            self._save_media(session, p["embed"], p["id"])

            reply_parent = ((p["reply"] or {}).get("parent") or {}).get("uri")
            if reply_parent:
                session.run(
                    """
                    MERGE (parent:BlueSky_Post {id: $parent_id})
                    ON CREATE SET parent.dateC = datetime()
                    WITH parent
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (p)-[:REPLY_TO]->(parent)
                    """,
                    {"post_id": p["id"], "parent_id": reply_parent},
                )

            reason_type = (reason or {}).get("$type", "")
            if reason_type.endswith("#reasonRepost"):
                by = (reason or {}).get("by", {})
                repost_actor = by.get("did")
                repost_time = (reason or {}).get("indexed_at") or indexed.isoformat()
                if repost_actor:
                    repost_id = f"repost://{repost_actor}/{p['id']}"
                    self._merge_account_stub(session, repost_actor)
                    session.run(
                        """
                        MERGE (rp:BlueSky_Post {id: $repost_id})
                        ON CREATE SET rp.dateC = datetime()
                        SET
                            rp.account_id = $actor_id,
                            rp.created_at = datetime($created_at),
                            rp.indexed_at = datetime($created_at),
                            rp.dateM = datetime(),
                            rp.synthetic = true
                        WITH rp
                        MATCH (orig:BlueSky_Post {id: $orig_id})
                        MERGE (rp)-[:REBLOG_OF]->(orig)
                        """,
                        {
                            "repost_id": repost_id,
                            "actor_id": repost_actor,
                            "created_at": repost_time,
                            "orig_id": p["id"],
                        },
                    )
                    session.run(
                        """
                        MATCH (a:BlueSky_Account {id: $actor_id})
                        MATCH (rp:BlueSky_Post {id: $repost_id})
                        MERGE (a)-[:POSTED]->(rp)
                        """,
                        {"actor_id": repost_actor, "repost_id": repost_id},
                    )

            return is_new

    def _save_media(self, session, embed: Dict[str, Any], post_id: str):
        if not embed:
            return

        etype = embed.get("$type", "")

        if etype.endswith("embed.images#view"):
            for image in embed.get("images", []):
                uri = image.get("fullsize") or image.get("thumb") or ""
                if not uri:
                    continue
                media_id = DataParser.media_id(f"img:{uri}")
                session.run(
                    """
                    MERGE (m:Photo {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'image', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": image.get("fullsize") or image.get("thumb"),
                        "preview": image.get("thumb"),
                        "description": image.get("alt", ""),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.video#view"):
            uri = embed.get("playlist") or embed.get("thumbnail") or ""
            if uri:
                media_id = DataParser.media_id(f"video:{uri}")
                session.run(
                    """
                    MERGE (m:Video {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'video', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": embed.get("playlist") or embed.get("thumbnail"),
                        "preview": embed.get("thumbnail"),
                        "description": embed.get("alt", ""),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.external#view"):
            external = embed.get("external", {})
            uri = external.get("uri")
            if uri:
                media_id = DataParser.media_id(f"ext:{uri}")
                session.run(
                    """
                    MERGE (m:File {id: $id})
                    ON CREATE SET m.dateC = datetime()
                    SET m.type = 'external', m.url = $url, m.preview_url = $preview, m.description = $description, m.dateM = datetime()
                    WITH m
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (m)-[:part]->(p)
                    """,
                    {
                        "id": media_id,
                        "url": uri,
                        "preview": external.get("thumb", ""),
                        "description": external.get("title", ""),
                        "post_id": post_id,
                    },
                )


# =============================================================================
# MAIN APP
# =============================================================================


class BlueskyParser:
    """Main app class."""

    def __init__(self, config: Config):
        self.config = config
        self.log = Logger("BlueskyParser", log_file=config.LOG_FILE, level=config.LOG_LEVEL)
        self.db = Neo4jManager(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD)
        self.bluesky = BlueskyClient(config.BLUESKY_IDENTIFIER, config.BLUESKY_PASSWORD)
        self.loader = GraphLoader(self.db, self.log)

    def add_accounts(self, actors: List[str], prior: int = 1) -> tuple:
        self.log.start(f"Add accounts: {actors}")
        added_count = 0
        updated_count = 0

        for actor in actors:
            self.log.process(f"Resolve account {actor}...")
            account = self.bluesky.get_account(actor)
            if account is None:
                self.log.error(f"Account not found: {actor}")
                continue

            self.log.success(f"Found: {account.get('display_name', '')} (@{account.get('handle', '')})")
            is_created = self.loader.add_account(account, prior)
            if is_created:
                added_count += 1
                self.log.success(f"Added account {actor}")
            else:
                updated_count += 1
                self.log.info(f"Updated account {actor}")

        self.log.end(f"Accounts done: added={added_count}, updated={updated_count}")
        return added_count, updated_count

    def fetch_posts_for_account(self, account_id: str) -> int:
        account_info = self.loader.get_account_info(account_id)
        if account_info is None:
            self.log.error(f"Account {account_id} not found in DB")
            return 0

        handle = account_info["handle"]
        last_post_uri = account_info["last_post_uri"]

        limits = []
        if last_post_uri:
            limits.append(f"last_post_uri={last_post_uri}")
        if self.config.MAX_POSTS > 0:
            limits.append(f"max_posts={self.config.MAX_POSTS}")
        if self.config.MAX_PAGES > 0:
            limits.append(f"max_pages={self.config.MAX_PAGES}")
        if self.config.MIN_DATE:
            limits.append(f"min_date={self.config.MIN_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

        limits_text = ", ".join(limits) if limits else "no limits"
        self.log.start(f"Fetch posts for @{handle} ({limits_text})")

        loaded_count = 0
        page_count = 0
        cursor = None
        first_post_uri = None
        stop_reason = None

        while True:
            if self.config.MAX_PAGES > 0 and page_count >= self.config.MAX_PAGES:
                stop_reason = f"page limit reached ({self.config.MAX_PAGES})"
                break

            try:
                self.log.process(f"Request feed page {page_count + 1}...")
                response = self.bluesky.get_author_feed(actor=account_id, cursor=cursor, limit=100)
                feed_items = response.get("feed", [])
                next_cursor = response.get("cursor")

                if not feed_items:
                    stop_reason = "feed exhausted"
                    self.log.success("No more feed items")
                    break

                self.log.success(f"Received {len(feed_items)} feed items")

            except Exception as exc:
                self.log.error(f"Feed request error: {exc}")
                self.log.info("Sleep 10s before retry...")
                time.sleep(10)
                continue

            should_break = False
            for item in feed_items:
                post = item.get("post", {})
                post_uri = post.get("uri")
                if not post_uri:
                    continue

                if last_post_uri and post_uri == last_post_uri:
                    stop_reason = f"reached last_post_uri={last_post_uri}"
                    self.log.success(stop_reason)
                    should_break = True
                    break

                record = post.get("record", {})
                created_at = DataParser.parse_iso_datetime(record.get("created_at") or post.get("indexed_at"))
                if self.config.MIN_DATE and created_at and created_at < self.config.MIN_DATE:
                    stop_reason = "min_date reached"
                    self.log.success(f"Reached min_date at {created_at.isoformat()}")
                    should_break = True
                    break

                if first_post_uri is None:
                    first_post_uri = post_uri
                    self.log.info(f"First post uri: {first_post_uri}")

                is_new = self.loader.save_post(post, reason=item.get("reason"))
                loaded_count += 1

                if is_new:
                    self.log.data(f"Loaded posts: {loaded_count} (new)")
                else:
                    self.log.info(f"Loaded posts: {loaded_count} (updated)")

                if self.config.MAX_POSTS > 0 and loaded_count >= self.config.MAX_POSTS:
                    stop_reason = f"post limit reached ({self.config.MAX_POSTS})"
                    self.log.success(stop_reason)
                    should_break = True
                    break

            page_count += 1

            if should_break:
                break

            if not next_cursor:
                stop_reason = "no next cursor"
                break

            cursor = next_cursor

        if first_post_uri:
            self.loader.update_account_last_post_uri(account_id, first_post_uri)
            self.log.info(f"Updated last_post_uri={first_post_uri} for {account_id}")

        result = f"Loaded {loaded_count} posts in {page_count} pages"
        if stop_reason:
            result += f" ({stop_reason})"
        self.log.end(result)

        return loaded_count

    def run(self):
        self.log.separator("#")
        self.log.start("SCRIPT START bluesky_pars.py")
        self.log.separator("#")

        start_time = datetime.datetime.now()

        self.log.info(">>> Step 1: Add accounts")
        self.log.info(f"Accounts list: {self.config.ACCOUNTS}")
        self.add_accounts(self.config.ACCOUNTS, prior=self.config.PRIOR)

        self.log.info(">>> Step 2: Load posts")
        self.log.info(
            f"Settings: max_pages={self.config.MAX_PAGES}, max_posts={self.config.MAX_POSTS}, "
            f"min_date={self.config.MIN_DATE}, prior={self.config.PRIOR}"
        )

        accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
        total_accounts = len(accounts)
        self.log.data(f"Accounts found: {total_accounts}")

        total_loaded = 0
        for idx, account in enumerate(accounts, 1):
            self.log.separator("=", 50)
            self.log.start(f"Process account {idx}/{total_accounts}")
            loaded = self.fetch_posts_for_account(account["id"])
            total_loaded += loaded
            self.log.end(f"Account {idx}/{total_accounts} done")

        end_time = datetime.datetime.now()
        stats = self.db.get_stats()

        self.log.separator("#")
        self.log.end("WORK FINISHED")
        self.log.separator("#")

        self.log.info(f"Execution time: {end_time - start_time}")
        self.log.data(f"Total loaded posts: {total_loaded}")
        self.log.separator("=")
        self.log.info("Graph stats:")
        for label, count in stats.items():
            self.log.data(f"  - {label}: {count}")
        self.log.separator("=")

        self.log.info("Dedup check:")
        self.log.success("  - No duplicates (MERGE by id)")
        self.log.success("  - Re-runs update existing posts")

    def close(self):
        self.db.close()


# =============================================================================
# ENTRYPOINT
# =============================================================================


if __name__ == "__main__":
    # Accounts to parse (handle, did, @handle, or bsky profile URL)
    ACCOUNTS_TO_PARSE = [
        "https://bsky.app/profile/jay.bsky.team",
        "https://bsky.app/profile/atproto.com",
        # "@handle.bsky.social",
        # "did:plc:...",
    ]

    # Loading options
    MAX_PAGES = 3
    MAX_POSTS = 0
    MIN_DATE = None
    PRIOR = 1

    # Example:
    # MIN_DATE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

    config = Config(
        ACCOUNTS=ACCOUNTS_TO_PARSE,
        MAX_PAGES=MAX_PAGES,
        MAX_POSTS=MAX_POSTS,
        MIN_DATE=MIN_DATE,
        PRIOR=PRIOR,
        # Set your auth:
        BLUESKY_IDENTIFIER="your_handle.bsky.social",
        BLUESKY_PASSWORD="your-app-password",
    )

    parser = BlueskyParser(config)
    try:
        parser.run()
    finally:
        parser.close()
