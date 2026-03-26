"""
BlueSky Parser - load posts from BlueSky into Neo4j graph DB

Graph structure:
    (BlueSky_Account)-[:POSTED]->(BlueSky_Post)
    (BlueSky_Account)-[:FOLLOWS]->(BlueSky_Account)
    (BlueSky_Account)-[:FOLLOWING]->(BlueSky_Account)
    (BlueSky_Account)-[:COMMENTED]->(BlueSky_Post)
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
from collections import Counter
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Set, Tuple
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

    # ВВЕСТИ СВОИ ДАННЫЕ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "**************")

    # BlueSky ВВЕСТИ СВОИ ДАННЫЕ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    BLUESKY_IDENTIFIER: str = os.getenv("BLUESKY_IDENTIFIER", "**************")
    BLUESKY_PASSWORD: str = os.getenv("BLUESKY_PASSWORD", "**************")

    # Loading limits
    MAX_PAGES: int = 0  # 0 = unlimited
    MAX_POSTS: int = 0  # 0 = unlimited
    MIN_DATE: Optional[datetime.datetime] = None
    MAX_FOLLOWERS: int = 0  # 0 = unlimited
    MAX_FOLLOWS: int = 0  # 0 = unlimited

    # Accounts to parse
    ACCOUNTS: List[str] = None
    PRIOR: int = 1
    SYNC_ACCOUNT_COUNTERS: bool = True
    SYNC_SOCIAL_GRAPH: bool = False
    SYNC_FOLLOWS_GRAPH: bool = False
    ENRICH_SOCIAL_PROFILES: bool = True
    SYNC_POST_COMMENTERS: bool = True
    COMMENT_THREAD_DEPTH: int = 16

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
            rel = session.run(
                "MATCH ()-[r]->() WHERE type(r) = $rel_type RETURN count(r) as count",
                {"rel_type": "FOLLOWS"},
            ).single()
            stats["FOLLOWS"] = rel["count"] if rel else 0
            rel = session.run(
                "MATCH ()-[r]->() WHERE type(r) = $rel_type RETURN count(r) as count",
                {"rel_type": "FOLLOWING"},
            ).single()
            stats["FOLLOWING"] = rel["count"] if rel else 0
            rel = session.run(
                "MATCH ()-[r]->() WHERE type(r) = $rel_type RETURN count(r) as count",
                {"rel_type": "COMMENTED"},
            ).single()
            stats["COMMENTED"] = rel["count"] if rel else 0
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
    def _as_string(value: Any) -> str:
        return value if isinstance(value, str) else ""

    @staticmethod
    def _normalize_account_id(value: Any) -> str:
        text = BlueskyClient._as_string(value).strip()
        if not text or text == "0" or text.isdigit():
            return ""
        return text

    @staticmethod
    def _pick_first(data: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    @classmethod
    def profile_url(cls, did: str, handle: str = "") -> str:
        actor = cls._as_string(handle).strip() or cls._as_string(did).strip()
        return f"https://bsky.app/profile/{actor}" if actor else ""

    @staticmethod
    def _to_snake_case(key: str) -> str:
        if not isinstance(key, str) or not key or key.startswith("$"):
            return key
        converted = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", key)
        converted = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", converted)
        return converted.lower()

    @staticmethod
    def _normalize_value(obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            normalized = {key: BlueskyClient._normalize_value(value) for key, value in obj.items()}
            for key, value in list(normalized.items()):
                alias = BlueskyClient._to_snake_case(key)
                if alias != key and alias not in normalized:
                    normalized[alias] = value
            return normalized
        if isinstance(obj, list):
            return [BlueskyClient._normalize_value(item) for item in obj]
        if hasattr(obj, "model_dump"):
            return BlueskyClient._normalize_value(obj.model_dump(by_alias=True, exclude_none=True))
        if hasattr(obj, "dict"):
            return BlueskyClient._normalize_value(obj.dict(by_alias=True, exclude_none=True))
        return obj

    @staticmethod
    def _to_dict(obj: Any) -> Any:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return {key: BlueskyClient._normalize_value(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [BlueskyClient._normalize_value(item) for item in obj]
        if hasattr(obj, "model_dump"):
            return BlueskyClient._normalize_value(obj.model_dump(by_alias=True, exclude_none=True))
        if hasattr(obj, "dict"):
            return BlueskyClient._normalize_value(obj.dict(by_alias=True, exclude_none=True))
        return dict(obj)

    def get_account(self, actor: str) -> Optional[Dict[str, Any]]:
        clean_actor = self.normalize_actor(actor)
        for attempt in range(3):
            try:
                profile = self.client.get_profile(actor=clean_actor)
                return self._to_dict(profile)
            except Exception:
                if attempt < 2:
                    time.sleep(0.4 * (attempt + 1))
        return None

    def get_author_feed(self, actor: str, cursor: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        try:
            clean_actor = self.normalize_actor(actor)
            response = self.client.get_author_feed(actor=clean_actor, cursor=cursor, limit=limit)
            return self._to_dict(response)
        except Exception as exc:
            raise Exception(f"Failed to fetch feed for {actor}: {exc}") from exc

    def get_post_thread(self, uri: str, depth: int = 16) -> Dict[str, Any]:
        try:
            clean_uri = uri.strip()
            try:
                response = self.client.get_post_thread(uri=clean_uri, depth=depth, parent_height=0)
            except TypeError:
                response = self.client.get_post_thread(uri=clean_uri, depth=depth)
            return self._to_dict(response)
        except Exception:
            return {}

    def get_social_graph(
            self,
            actor: str,
            limit: int = 100,
            max_follows: int = 0,
            max_followers: int = 0,
            enrich_profiles: bool = True,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        clean_actor = self.normalize_actor(actor)
        follows = self._paginate_people(
            clean_actor,
            "get_follows",
            "follows",
            limit=limit,
            max_items=max_follows,
            enrich_profiles=enrich_profiles,
        )
        followers = self._paginate_people(
            clean_actor,
            "get_followers",
            "followers",
            limit=limit,
            max_items=max_followers,
            enrich_profiles=enrich_profiles,
        )
        return follows, followers

    def get_followers_graph(
            self,
            actor: str,
            limit: int = 100,
            max_items: int = 0,
            enrich_profiles: bool = True,
    ) -> List[Dict[str, Any]]:
        clean_actor = self.normalize_actor(actor)
        return self._paginate_people(
            clean_actor,
            "get_followers",
            "followers",
            limit=limit,
            max_items=max_items,
            enrich_profiles=enrich_profiles,
        )

    def get_follows_graph(
            self,
            actor: str,
            limit: int = 100,
            max_items: int = 0,
            enrich_profiles: bool = True,
    ) -> List[Dict[str, Any]]:
        clean_actor = self.normalize_actor(actor)
        return self._paginate_people(
            clean_actor,
            "get_follows",
            "follows",
            limit=limit,
            max_items=max_items,
            enrich_profiles=enrich_profiles,
        )

    def _paginate_people(
            self,
            actor: str,
            method_name: str,
            field_name: str,
            limit: int = 100,
            max_items: int = 0,
            enrich_profiles: bool = True,
    ) -> List[Dict[str, Any]]:
        cursor = None
        items: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        while True:
            method = getattr(self.client, method_name)
            response = self._to_dict(method(actor=actor, cursor=cursor, limit=limit))
            for person in response.get(field_name, []):
                p = self._to_dict(person)
                did = self._normalize_account_id(p.get("did"))
                if not did or did in seen:
                    continue
                seen.add(did)
                profile = p
                if enrich_profiles:
                    lookup_actor = self._as_string(self._pick_first(p, "handle", "did")) or did
                    detailed = self.get_account(lookup_actor)
                    if not detailed and lookup_actor != did:
                        detailed = self.get_account(did)
                    if detailed:
                        profile = {**p, **detailed}
                handle = self._as_string(profile.get("handle"))
                description = self._as_string(self._pick_first(profile, "description", "desc", "bio"))
                avatar_url = self._as_string(self._pick_first(profile, "avatar_url", "avatar"))
                banner_url = self._as_string(self._pick_first(profile, "banner_url", "banner"))
                items.append(
                    {
                        "did": did,
                        "handle": handle,
                        "display_name": self._as_string(self._pick_first(profile, "display_name", "displayName")),
                        "description": description,
                        "bio": description,
                        "followers_count": self._pick_first(
                            profile, "followers_count", "followersCount", "follower_count", "followers"
                        ),
                        "following_count": self._pick_first(
                            profile,
                            "following_count",
                            "followingCount",
                            "follows_count",
                            "followsCount",
                            "follows",
                            "following",
                        ),
                        "posts_count": self._pick_first(profile, "posts_count", "postsCount", "posts"),
                        "created_at": self._as_string(self._pick_first(profile, "created_at", "indexed_at")),
                        "avatar_url": avatar_url,
                        "avatar": avatar_url,
                        "banner_url": banner_url,
                        "banner": banner_url,
                        "profile_url": self.profile_url(did=did, handle=handle),
                        "url": self.profile_url(did=did, handle=handle),
                    }
                )
                if max_items > 0 and len(items) >= max_items:
                    return items
            cursor = response.get("cursor")
            if not cursor:
                break
        return items


# =============================================================================
# DATA PARSER
# =============================================================================


class DataParser:
    """Helpers for post text/facets/embed parsing."""

    URI_RE = re.compile(r"https?://[^\s)\]}>\"']+")
    HASHTAG_RE = re.compile(r"(?<!\w)#([A-Za-z0-9_]{1,64})")

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

    @classmethod
    def extract_tags_from_text(cls, text: str) -> List[str]:
        return list(dict.fromkeys(cls.HASHTAG_RE.findall(text or "")))

    @staticmethod
    def media_id(seed: str) -> str:
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    @staticmethod
    def as_string(value: Any) -> str:
        return value if isinstance(value, str) else ""

    @staticmethod
    def normalize_account_id(value: Any) -> str:
        text = DataParser.as_string(value).strip()
        if not text or text == "0" or text.isdigit():
            return ""
        return text


# =============================================================================
# GRAPH LOADER
# =============================================================================


class GraphLoader:
    """Loads BlueSky entities into Neo4j."""

    def __init__(self, db: Neo4jManager, logger: Logger):
        self.db = db
        self.log = logger
        self._embed_debug_count = 0

    @staticmethod
    def _pick_first(data: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _profile_url(did: str, handle: str = "") -> str:
        actor = DataParser.as_string(handle).strip() or DataParser.as_string(did).strip()
        return f"https://bsky.app/profile/{actor}" if actor else ""

    def _copy_parts_to_post(self, session, source_post_id: str, target_post_id: str):
        """Copy all :part relations from one post to another."""
        session.run(
            """
            MATCH (src:BlueSky_Post {id: $source_post_id})
            MATCH (dst:BlueSky_Post {id: $target_post_id})
            MATCH (m)-[:part]->(src)
            MERGE (m)-[:part]->(dst)
            """,
            {"source_post_id": source_post_id, "target_post_id": target_post_id},
        )

    def add_account(self, account: Dict[str, Any], prior: int = 1) -> bool:
        created_at = account.get("created_at") or account.get("indexed_at")
        created_at = DataParser.parse_iso_datetime(created_at)
        created_at_iso = (created_at or datetime.datetime.utcnow()).isoformat()
        did = DataParser.normalize_account_id(account.get("did"))
        if not did:
            return False
        handle = DataParser.as_string(account.get("handle"))
        display_name = DataParser.as_string(self._pick_first(account, "display_name", "displayName"))
        description = DataParser.as_string(self._pick_first(account, "description", "desc", "bio"))
        avatar_url = DataParser.as_string(self._pick_first(account, "avatar_url", "avatar"))
        banner_url = DataParser.as_string(self._pick_first(account, "banner_url", "banner"))
        profile_url = self._profile_url(did=did, handle=handle)
        followers_count = self._to_int_or_none(
            self._pick_first(account, "followers_count", "followersCount", "follower_count", "followers")
        )
        following_count = self._to_int_or_none(
            self._pick_first(
                account,
                "following_count",
                "followingCount",
                "follows_count",
                "followsCount",
                "follows",
                "following",
            )
        )
        posts_count = self._to_int_or_none(self._pick_first(account, "posts_count", "postsCount", "posts"))

        query = """
        MERGE (a:BlueSky_Account {id: $id})
        ON CREATE SET
            a.dateC = datetime(),
            a.handle = $handle,
            a.display_name = $display_name,
            a.description = $description,
            a.descr = $description,
            a.url = $url,
            a.profile_url = $profile_url,
            a.avatar_url = $avatar_url,
            a.avatar = $avatar_url,
            a.banner_url = $banner_url,
            a.banner = $banner_url,
            a.created_at = datetime($created_at),
            a.followers_count = coalesce($followers_count, 0),
            a.following_count = coalesce($following_count, 0),
            a.follows_count = coalesce($following_count, 0),
            a.posts_count = coalesce($posts_count, 0),
            a.prior = $prior,
            a.last_post_uri = ''
        ON MATCH SET
            a.handle = CASE WHEN $handle <> '' THEN $handle ELSE a.handle END,
            a.display_name = CASE WHEN $display_name <> '' THEN $display_name ELSE a.display_name END,
            a.description = CASE WHEN $description <> '' THEN $description ELSE a.description END,
            a.descr = CASE WHEN $description <> '' THEN $description ELSE a.descr END,
            a.url = CASE WHEN $url <> '' THEN $url ELSE a.url END,
            a.profile_url = CASE WHEN $profile_url <> '' THEN $profile_url ELSE a.profile_url END,
            a.avatar_url = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar_url END,
            a.avatar = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar END,
            a.banner_url = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner_url END,
            a.banner = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner END,
            a.followers_count = CASE WHEN $followers_count IS NOT NULL THEN $followers_count ELSE a.followers_count END,
            a.following_count = CASE WHEN $following_count IS NOT NULL THEN $following_count ELSE a.following_count END,
            a.follows_count = CASE WHEN $following_count IS NOT NULL THEN $following_count ELSE a.follows_count END,
            a.posts_count = CASE WHEN $posts_count IS NOT NULL THEN $posts_count ELSE a.posts_count END,
            a.prior = $prior,
            a.dateM = datetime()
        """
        params = {
            "id": did,
            "handle": handle,
            "display_name": display_name,
            "description": description,
            "url": profile_url,
            "profile_url": profile_url,
            "avatar_url": avatar_url,
            "banner_url": banner_url,
            "created_at": created_at_iso,
            "followers_count": followers_count,
            "following_count": following_count,
            "posts_count": posts_count,
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
        did = DataParser.normalize_account_id(did)
        if not did:
            return
        profile_url = self._profile_url(did=did)
        query = """
        MERGE (a:BlueSky_Account {id: $did})
        ON CREATE SET
            a.dateC = datetime(),
            a.handle = $did,
            a.display_name = '',
            a.description = '',
            a.descr = '',
            a.url = $url,
            a.profile_url = $profile_url,
            a.avatar_url = '',
            a.avatar = '',
            a.banner_url = '',
            a.banner = '',
            a.created_at = datetime(),
            a.followers_count = 0,
            a.following_count = 0,
            a.follows_count = 0,
            a.posts_count = 0,
            a.prior = 0,
            a.last_post_uri = ''
        ON MATCH SET
            a.url = CASE WHEN $url <> '' THEN $url ELSE a.url END,
            a.profile_url = CASE WHEN $profile_url <> '' THEN $profile_url ELSE a.profile_url END,
            a.dateM = datetime()
        """
        session.run(query, {"did": did, "url": profile_url, "profile_url": profile_url})

    def _merge_account_meta(self, session, profile: Dict[str, Any]):
        did = DataParser.normalize_account_id(profile.get("did"))
        if not did:
            return

        handle = DataParser.as_string(profile.get("handle"))
        display_name = DataParser.as_string(profile.get("display_name", profile.get("displayName", "")))
        description = DataParser.as_string(self._pick_first(profile, "description", "desc", "bio"))
        avatar_url = DataParser.as_string(self._pick_first(profile, "avatar_url", "avatar"))
        banner_url = DataParser.as_string(self._pick_first(profile, "banner_url", "banner"))
        profile_url = self._profile_url(did=did, handle=handle)
        followers_count = self._to_int_or_none(
            self._pick_first(profile, "followers_count", "followersCount", "follower_count", "followers")
        )
        following_count = self._to_int_or_none(
            self._pick_first(
                profile,
                "following_count",
                "followingCount",
                "follows_count",
                "followsCount",
                "follows",
                "following",
            )
        )
        posts_count = self._to_int_or_none(self._pick_first(profile, "posts_count", "postsCount", "posts"))
        created_at_value = profile.get("created_at", profile.get("indexed_at"))
        created_at = DataParser.parse_iso_datetime(created_at_value)
        created_at_iso = (created_at or datetime.datetime.utcnow()).isoformat()

        query = """
        MERGE (a:BlueSky_Account {id: $did})
        ON CREATE SET
            a.dateC = datetime(),
            a.handle = CASE WHEN $handle <> '' THEN $handle ELSE $did END,
            a.display_name = $display_name,
            a.description = $description,
            a.descr = $description,
            a.url = $url,
            a.profile_url = $profile_url,
            a.avatar_url = $avatar_url,
            a.avatar = $avatar_url,
            a.banner_url = $banner_url,
            a.banner = $banner_url,
            a.created_at = datetime($created_at),
            a.followers_count = coalesce($followers_count, 0),
            a.following_count = coalesce($following_count, 0),
            a.follows_count = coalesce($following_count, 0),
            a.posts_count = coalesce($posts_count, 0),
            a.prior = 0,
            a.last_post_uri = ''
        ON MATCH SET
            a.handle = CASE WHEN $handle <> '' THEN $handle ELSE a.handle END,
            a.display_name = CASE WHEN $display_name <> '' THEN $display_name ELSE a.display_name END,
            a.description = CASE WHEN $description <> '' THEN $description ELSE a.description END,
            a.descr = CASE WHEN $description <> '' THEN $description ELSE a.descr END,
            a.url = CASE WHEN $url <> '' THEN $url ELSE a.url END,
            a.profile_url = CASE WHEN $profile_url <> '' THEN $profile_url ELSE a.profile_url END,
            a.avatar_url = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar_url END,
            a.avatar = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar END,
            a.banner_url = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner_url END,
            a.banner = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner END,
            a.followers_count = CASE WHEN $followers_count IS NOT NULL THEN $followers_count ELSE a.followers_count END,
            a.following_count = CASE WHEN $following_count IS NOT NULL THEN $following_count ELSE a.following_count END,
            a.follows_count = CASE WHEN $following_count IS NOT NULL THEN $following_count ELSE a.follows_count END,
            a.posts_count = CASE WHEN $posts_count IS NOT NULL THEN $posts_count ELSE a.posts_count END,
            a.dateM = datetime()
        """
        session.run(
            query,
            {
                "did": did,
                "handle": handle,
                "display_name": display_name,
                "description": description,
                "url": profile_url,
                "profile_url": profile_url,
                "avatar_url": avatar_url,
                "banner_url": banner_url,
                "created_at": created_at_iso,
                "followers_count": followers_count,
                "following_count": following_count,
                "posts_count": posts_count,
            },
        )

    def save_social_graph(self, account_did: str, follows: List[Dict[str, Any]], followers: List[Dict[str, Any]]):
        with self.db.driver.session() as session:
            self._merge_account_stub(session, account_did)

            for followee in follows:
                did = followee.get("did")
                if not did:
                    continue
                self._merge_account_meta(session, followee)
                session.run(
                    """
                    MATCH (src:BlueSky_Account {id: $src_id})
                    MATCH (dst:BlueSky_Account {id: $dst_id})
                    MERGE (src)-[:FOLLOWING]->(dst)
                    """,
                    {"src_id": account_did, "dst_id": did},
                )

            for follower in followers:
                did = follower.get("did")
                if not did:
                    continue
                self._merge_account_meta(session, follower)
                session.run(
                    """
                    MATCH (src:BlueSky_Account {id: $src_id})
                    MATCH (dst:BlueSky_Account {id: $dst_id})
                    MERGE (src)-[:FOLLOWS]->(dst)
                    """,
                    {"src_id": did, "dst_id": account_did},
                )

            session.run(
                """
                MATCH (a:BlueSky_Account {id: $account_id})
                SET
                    a.following_loaded = $following_loaded,
                    a.follows_loaded = $following_loaded,
                    a.followers_loaded = $followers_loaded,
                    a.dateM = datetime()
                """,
                {
                    "account_id": account_did,
                    "following_loaded": len(follows),
                    "followers_loaded": len(followers),
                },
            )

    def save_followers_graph(self, account_did: str, followers: List[Dict[str, Any]]):
        with self.db.driver.session() as session:
            self._merge_account_stub(session, account_did)

            for follower in followers:
                did = follower.get("did")
                if not did:
                    continue
                self._merge_account_meta(session, follower)
                session.run(
                    """
                    MATCH (src:BlueSky_Account {id: $src_id})
                    MATCH (dst:BlueSky_Account {id: $dst_id})
                    MERGE (src)-[:FOLLOWS]->(dst)
                    """,
                    {"src_id": did, "dst_id": account_did},
                )

            session.run(
                """
                MATCH (a:BlueSky_Account {id: $account_id})
                SET a.followers_loaded = $followers_loaded, a.dateM = datetime()
                """,
                {"account_id": account_did, "followers_loaded": len(followers)},
            )

    def save_follows_graph(self, account_did: str, follows: List[Dict[str, Any]]):
        with self.db.driver.session() as session:
            self._merge_account_stub(session, account_did)

            for followee in follows:
                did = followee.get("did")
                if not did:
                    continue
                self._merge_account_meta(session, followee)
                session.run(
                    """
                    MATCH (src:BlueSky_Account {id: $src_id})
                    MATCH (dst:BlueSky_Account {id: $dst_id})
                    MERGE (src)-[:FOLLOWING]->(dst)
                    """,
                    {"src_id": account_did, "dst_id": did},
                )

            session.run(
                """
                MATCH (a:BlueSky_Account {id: $account_id})
                SET
                    a.following_loaded = $following_loaded,
                    a.follows_loaded = $following_loaded,
                    a.dateM = datetime()
                """,
                {"account_id": account_did, "following_loaded": len(follows)},
            )

    def _extract_commenters_from_thread(
            self,
            thread_payload: Dict[str, Any],
            root_post_id: str,
    ) -> Tuple[Counter, Dict[str, Dict[str, str]], List[Dict[str, str]]]:
        actor_counts: Counter = Counter()
        actor_meta: Dict[str, Dict[str, str]] = {}
        comments: List[Dict[str, str]] = []
        seen_comment_posts: Set[str] = set()

        stack: List[Any] = []
        replies = thread_payload.get("replies", [])
        if isinstance(replies, list):
            stack.extend(replies)

        while stack:
            item = stack.pop()

            if not isinstance(item, dict):
                continue

            post = item.get("post")
            if isinstance(post, dict):
                comment_post_id = DataParser.as_string(post.get("uri"))
                author = post.get("author", {})
                if not isinstance(author, dict):
                    author = {}
                did = DataParser.normalize_account_id(author.get("did"))
                if did and comment_post_id and comment_post_id != root_post_id and comment_post_id not in seen_comment_posts:
                    seen_comment_posts.add(comment_post_id)
                    actor_counts[did] += 1
                    handle = DataParser.as_string(author.get("handle"))
                    display_name = DataParser.as_string(author.get("display_name") or author.get("displayName"))
                    prev = actor_meta.get(did, {})
                    actor_meta[did] = {
                        "handle": handle or prev.get("handle", ""),
                        "display_name": display_name or prev.get("display_name", ""),
                    }
                    comments.append(
                        {
                            "post_id": comment_post_id,
                            "actor_id": did,
                        }
                    )

            nested_replies = item.get("replies", [])
            if isinstance(nested_replies, list):
                stack.extend(nested_replies)

        return actor_counts, actor_meta, comments

    def save_post_commenters(self, root_post_id: str, thread_payload: Dict[str, Any]) -> Dict[str, int]:
        if not root_post_id or not thread_payload:
            return {"commenters": 0, "comments": 0}

        thread = thread_payload.get("thread", thread_payload)
        if not isinstance(thread, dict):
            return {"commenters": 0, "comments": 0}

        actor_counts, actor_meta, comments = self._extract_commenters_from_thread(thread, root_post_id)
        if not comments:
            return {"commenters": 0, "comments": 0}

        with self.db.driver.session() as session:
            session.run(
                """
                MERGE (p:BlueSky_Post {id: $post_id})
                ON CREATE SET p.dateC = datetime()
                """,
                {"post_id": root_post_id},
            )

            for actor_id, comments_count in actor_counts.items():
                meta = actor_meta.get(actor_id, {})
                self._merge_account_meta(
                    session,
                    {
                        "did": actor_id,
                        "handle": meta.get("handle", ""),
                        "display_name": meta.get("display_name", ""),
                    },
                )
                session.run(
                    """
                    MATCH (a:BlueSky_Account {id: $actor_id})
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (a)-[r:COMMENTED]->(p)
                    ON CREATE SET r.dateC = datetime()
                    SET r.comments_count = $comments_count, r.dateM = datetime()
                    """,
                    {"actor_id": actor_id, "post_id": root_post_id, "comments_count": comments_count},
                )

            for comment in comments:
                session.run(
                    """
                    MERGE (cp:BlueSky_Post {id: $comment_post_id})
                    ON CREATE SET cp.dateC = datetime(), cp.synthetic = true
                    SET cp.account_id = $actor_id, cp.dateM = datetime()
                    WITH cp
                    MATCH (p:BlueSky_Post {id: $post_id})
                    MERGE (cp)-[:REPLY_TO]->(p)
                    """,
                    {
                        "comment_post_id": comment["post_id"],
                        "actor_id": comment["actor_id"],
                        "post_id": root_post_id,
                    },
                )
                session.run(
                    """
                    MATCH (a:BlueSky_Account {id: $actor_id})
                    MATCH (cp:BlueSky_Post {id: $comment_post_id})
                    MERGE (a)-[:POSTED]->(cp)
                    """,
                    {"actor_id": comment["actor_id"], "comment_post_id": comment["post_id"]},
                )

            session.run(
                """
                MATCH (p:BlueSky_Post {id: $post_id})
                SET
                    p.commenters_count = $commenters_count,
                    p.comments_loaded = $comments_loaded,
                    p.dateM = datetime()
                """,
                {
                    "post_id": root_post_id,
                    "commenters_count": len(actor_counts),
                    "comments_loaded": len(comments),
                },
            )

        return {"commenters": len(actor_counts), "comments": len(comments)}

    def _extract_post_identity(self, post: Dict[str, Any]) -> Dict[str, Any]:
        author = post.get("author", {})
        record = post.get("record", {})
        text = record.get("text", "") or post.get("text", "")
        post_embed = post.get("embed")
        record_embed = record.get("embed")
        if post_embed and record_embed:
            embed = [post_embed, record_embed]
        else:
            embed = post_embed or record_embed or {}
        return {
            "id": post.get("uri"),
            "cid": post.get("cid"),
            "author_id": DataParser.normalize_account_id(author.get("did")),
            "author_handle": author.get("handle", ""),
            "author_display_name": author.get("display_name", author.get("displayName", "")),
            "author_avatar": author.get("avatar", ""),
            "author_banner": author.get("banner", ""),
            "created_at": record.get("created_at") or post.get("indexed_at"),
            "indexed_at": post.get("indexed_at"),
            "text": text,
            "reply": record.get("reply", {}),
            "facets": record.get("facets", []),
            "embed": embed,
            "reply_count": int(post.get("reply_count") or 0),
            "repost_count": int(post.get("repost_count") or 0),
            "like_count": int(post.get("like_count") or 0),
            "quote_count": int(post.get("quote_count") or 0),
            "view_count": int(post.get("view_count") or post.get("views") or 0),
        }

    def save_post(self, post: Dict[str, Any], reason: Optional[Dict[str, Any]] = None) -> bool:
        post = BlueskyClient._to_dict(post) or {}
        reason = BlueskyClient._to_dict(reason) or {}
        p = self._extract_post_identity(post)
        if not p["id"] or not p["author_id"]:
            return False

        created = DataParser.parse_iso_datetime(p["created_at"]) or datetime.datetime.utcnow()
        indexed = DataParser.parse_iso_datetime(p["indexed_at"]) or created

        facets_data = DataParser.parse_facets(p["facets"])
        text_tags = DataParser.extract_tags_from_text(p["text"])
        links = set(facets_data["links"]) | set(DataParser.extract_links_from_text(p["text"]))
        tags = set(facets_data["tags"]) | set(text_tags)

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
                a.profile_url = $profile_url,
                a.avatar_url = $avatar_url,
                a.avatar = $avatar_url,
                a.banner_url = $banner_url,
                a.banner = $banner_url,
                a.last_post_uri = ''
            ON MATCH SET
                a.handle = CASE WHEN $handle <> '' THEN $handle ELSE a.handle END,
                a.display_name = CASE WHEN $display_name <> '' THEN $display_name ELSE a.display_name END,
                a.url = CASE WHEN $url <> '' THEN $url ELSE a.url END,
                a.profile_url = CASE WHEN $profile_url <> '' THEN $profile_url ELSE a.profile_url END,
                a.avatar_url = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar_url END,
                a.avatar = CASE WHEN $avatar_url <> '' THEN $avatar_url ELSE a.avatar END,
                a.banner_url = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner_url END,
                a.banner = CASE WHEN $banner_url <> '' THEN $banner_url ELSE a.banner END,
                a.dateM = datetime()
            """
            author_profile_url = self._profile_url(did=p["author_id"], handle=p["author_handle"])
            session.run(
                account_merge,
                {
                    "id": p["author_id"],
                    "handle": p["author_handle"],
                    "display_name": p["author_display_name"],
                    "url": author_profile_url,
                    "profile_url": author_profile_url,
                    "avatar_url": DataParser.as_string(p.get("author_avatar")),
                    "banner_url": DataParser.as_string(p.get("author_banner")),
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
                p.comments_count = $reply_count,
                p.repost_count = $repost_count,
                p.reposts_count = $repost_count,
                p.like_count = $like_count,
                p.likes_count = $like_count,
                p.quote_count = $quote_count,
                p.view_count = $view_count,
                p.views_count = $view_count,
                p.text = $text,
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
                    "view_count": p["view_count"],
                    "text": p["text"][:8219],
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

            for tag in tags:
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

            reason_type = reason.get("$type", "")
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
                            rp.cid = $cid,
                            rp.account_id = $actor_id,
                            rp.created_at = datetime($created_at),
                            rp.indexed_at = datetime($created_at),
                            rp.text = $text,
                            rp.original_post_id = $orig_id,
                            rp.dateM = datetime(),
                            rp.synthetic = true
                        WITH rp
                        MATCH (orig:BlueSky_Post {id: $orig_id})
                        MERGE (rp)-[:REBLOG_OF]->(orig)
                        """,
                        {
                            "repost_id": repost_id,
                            "cid": p["cid"],
                            "actor_id": repost_actor,
                            "created_at": repost_time,
                            "text": p["text"][:8219],
                            "orig_id": p["id"],
                        },
                    )
                    self._copy_parts_to_post(session, p["id"], repost_id)
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

        if self._embed_debug_count < 10:
            blocks = self._iter_embeds(embed)
            types = [block.get("$type", "<no-type>") for block in blocks]
            self.log.info(f"Embed debug for {post_id}: {types if types else 'no parsed embed types'}")
            self._embed_debug_count += 1
        else:
            blocks = self._iter_embeds(embed)

        for block in blocks:
            self._save_media_from_embed(session, block, post_id)

    def _save_media_from_embed(self, session, embed: Dict[str, Any], post_id: str):
        etype = embed.get("$type", "")

        if etype.endswith("embed.images#view"):
            for image in embed.get("images", []):
                uri = DataParser.as_string(image.get("fullsize")) or DataParser.as_string(image.get("thumb"))
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
                        "url": uri,
                        "preview": DataParser.as_string(image.get("thumb")),
                        "description": DataParser.as_string(image.get("alt")),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.video#view"):
            uri = DataParser.as_string(embed.get("playlist")) or DataParser.as_string(embed.get("thumbnail"))
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
                        "url": uri,
                        "preview": DataParser.as_string(embed.get("thumbnail")),
                        "description": DataParser.as_string(embed.get("alt")),
                        "post_id": post_id,
                    },
                )

        elif etype.endswith("embed.external#view") or etype == "app.bsky.embed.external":
            external = embed.get("external", {})
            uri = DataParser.as_string(external.get("uri"))
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
                        "preview": DataParser.as_string(external.get("thumb")),
                        "description": DataParser.as_string(external.get("title")) or DataParser.as_string(
                            external.get("description")),
                        "post_id": post_id,
                    },
                )

    def _iter_embeds(self, root: Any) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        stack: List[Any] = [root]
        seen: Set[int] = set()

        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue

            item_id = id(item)
            if item_id in seen:
                continue
            seen.add(item_id)

            etype = item.get("$type", "")
            if etype.startswith("app.bsky.embed."):
                result.append(item)

            for value in item.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)

        return result


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
        self.run_account_ids: List[str] = []

    def add_accounts(self, actors: List[str], prior: int = 1) -> tuple:
        self.log.start(f"Add accounts: {actors}")
        added_count = 0
        updated_count = 0
        resolved_ids: List[str] = []

        for actor in actors:
            self.log.process(f"Resolve account {actor}...")
            account = self.bluesky.get_account(actor)
            if account is None:
                self.log.error(f"Account not found: {actor}")
                continue

            display_name = account.get("display_name") or account.get("displayName") or ""
            self.log.success(f"Found: {display_name} (@{account.get('handle', '')})")
            resolved_did = DataParser.normalize_account_id(account.get("did"))
            if resolved_did:
                resolved_ids.append(resolved_did)
            is_created = self.loader.add_account(account, prior)
            if is_created:
                added_count += 1
                self.log.success(f"Added account {actor}")
            else:
                updated_count += 1
                self.log.info(f"Updated account {actor}")

        self.run_account_ids = list(dict.fromkeys(resolved_ids))
        self.log.end(
            f"Accounts done: added={added_count}, updated={updated_count}, resolved={len(self.run_account_ids)}"
        )
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

                if first_post_uri is None:
                    first_post_uri = post_uri
                    self.log.info(f"First post uri: {first_post_uri}")

                if last_post_uri and post_uri == last_post_uri:
                    self.loader.save_post(post, reason=item.get("reason"))
                    self.log.info(f"Refreshed metrics for checkpoint post {post_uri}")

                    if self.config.SYNC_POST_COMMENTERS:
                        try:
                            thread = self.bluesky.get_post_thread(post_uri, depth=self.config.COMMENT_THREAD_DEPTH)
                            if thread:
                                comment_stats = self.loader.save_post_commenters(post_uri, thread)
                                if comment_stats["comments"] > 0:
                                    self.log.data(
                                        f"Comments synced for checkpoint post: commenters={comment_stats['commenters']}, "
                                        f"comments={comment_stats['comments']}"
                                    )
                        except Exception as exc:
                            self.log.warning(f"Failed to sync commenters for checkpoint post {post_uri}: {exc}")

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

                is_new = self.loader.save_post(post, reason=item.get("reason"))
                loaded_count += 1

                if is_new:
                    self.log.data(f"Loaded posts: {loaded_count} (new)")
                else:
                    self.log.info(f"Loaded posts: {loaded_count} (updated)")

                if self.config.SYNC_POST_COMMENTERS:
                    try:
                        thread = self.bluesky.get_post_thread(post_uri, depth=self.config.COMMENT_THREAD_DEPTH)
                        if thread:
                            comment_stats = self.loader.save_post_commenters(post_uri, thread)
                            if comment_stats["comments"] > 0:
                                self.log.data(
                                    f"Comments synced for post: commenters={comment_stats['commenters']}, "
                                    f"comments={comment_stats['comments']}"
                                )
                    except Exception as exc:
                        self.log.warning(f"Failed to sync commenters for post {post_uri}: {exc}")

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

    def sync_account_counters_for_account(self, account_id: str):
        self.log.process(f"Sync counters for {account_id}...")
        account = self.bluesky.get_account(account_id)
        if not account:
            self.log.warning(f"Failed to fetch account counters for {account_id}")
            return

        self.loader.add_account(account, self.config.PRIOR)
        followers = account.get("followers_count", account.get("followersCount", 0))
        following = account.get("following_count", account.get("follows_count", account.get("followsCount", 0)))
        posts = account.get("posts_count", account.get("postsCount", 0))
        self.log.data(f"Counters for {account_id}: followers={followers}, following={following}, posts={posts}")

    def sync_social_graph_for_account(self, account_id: str):
        follower_limit = self.config.MAX_FOLLOWERS if self.config.MAX_FOLLOWERS > 0 else 0
        suffix = f" (limit={follower_limit})" if follower_limit > 0 else ""
        self.log.process(f"Sync followers relations for {account_id}{suffix}...")
        account = self.bluesky.get_account(account_id)
        if account:
            self.loader.add_account(account, self.config.PRIOR)
        followers = self.bluesky.get_followers_graph(
            account_id,
            max_items=follower_limit,
            enrich_profiles=self.config.ENRICH_SOCIAL_PROFILES,
        )
        self.loader.save_followers_graph(account_id, followers)
        self.log.data(f"Followers relations for {account_id}: followers={len(followers)}")

    def sync_follows_graph_for_account(self, account_id: str):
        follows_limit = self.config.MAX_FOLLOWS if self.config.MAX_FOLLOWS > 0 else 0
        suffix = f" (limit={follows_limit})" if follows_limit > 0 else ""
        self.log.process(f"Sync follows relations for {account_id}{suffix}...")
        account = self.bluesky.get_account(account_id)
        if account:
            self.loader.add_account(account, self.config.PRIOR)
        follows = self.bluesky.get_follows_graph(
            account_id,
            max_items=follows_limit,
            enrich_profiles=self.config.ENRICH_SOCIAL_PROFILES,
        )
        self.loader.save_follows_graph(account_id, follows)
        self.log.data(f"Follows relations for {account_id}: follows={len(follows)}")

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
            f"min_date={self.config.MIN_DATE}, prior={self.config.PRIOR}, "
            f"max_followers={self.config.MAX_FOLLOWERS}, max_follows={self.config.MAX_FOLLOWS}, "
            f"enrich_social_profiles={self.config.ENRICH_SOCIAL_PROFILES}, "
            f"sync_account_counters={self.config.SYNC_ACCOUNT_COUNTERS}, "
            f"sync_social_graph={self.config.SYNC_SOCIAL_GRAPH}, "
            f"sync_follows_graph={self.config.SYNC_FOLLOWS_GRAPH}, "
            f"sync_post_commenters={self.config.SYNC_POST_COMMENTERS}, "
            f"comment_thread_depth={self.config.COMMENT_THREAD_DEPTH}"
        )

        if self.run_account_ids:
            accounts = [{"id": did, "last_post_uri": ""} for did in self.run_account_ids]
            self.log.data(f"Accounts selected from current run: {len(accounts)}")
        else:
            accounts = self.db.get_accounts_with_prior(prior=self.config.PRIOR)
            self.log.data(f"Accounts found by prior={self.config.PRIOR}: {len(accounts)}")
        filtered_accounts: List[Dict[str, Any]] = []
        for account in accounts:
            normalized_id = DataParser.normalize_account_id(account.get("id"))
            if not normalized_id:
                self.log.warning(f"Skip account with invalid id: {account.get('id')}")
                continue
            filtered_accounts.append({"id": normalized_id, "last_post_uri": account.get("last_post_uri", "")})
        accounts = filtered_accounts
        total_accounts = len(accounts)

        if self.config.SYNC_ACCOUNT_COUNTERS:
            self.log.info(">>> Step 2.1: Sync account counters")
            for account in accounts:
                try:
                    self.sync_account_counters_for_account(account["id"])
                except Exception as exc:
                    self.log.warning(f"Failed to sync account counters for {account['id']}: {exc}")

        if self.config.SYNC_SOCIAL_GRAPH:
            self.log.info(">>> Step 2.2: Sync followers relations")
            for account in accounts:
                try:
                    self.sync_social_graph_for_account(account["id"])
                except Exception as exc:
                    self.log.warning(f"Failed to sync followers relations for {account['id']}: {exc}")

        if self.config.SYNC_FOLLOWS_GRAPH:
            self.log.info(">>> Step 2.3: Sync follows relations")
            for account in accounts:
                try:
                    self.sync_follows_graph_for_account(account["id"])
                except Exception as exc:
                    self.log.warning(f"Failed to sync follows relations for {account['id']}: {exc}")

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


def prompt_accounts(default_accounts: List[str]) -> List[str]:
    """Ask user which BlueSky accounts to parse."""
    print("\nBlueSky accounts to parse.")
    print("You can enter a handle, @handle, DID, or full profile URL.")
    print("Examples: jay.bsky.team  @atproto.com  https://bsky.app/profile/jay.bsky.team")
    print(f"Default: {', '.join(default_accounts)}")

    try:
        manual_input = input(
            "Enter one or more accounts separated by comma or space, or press Enter to use default: "
        ).strip()
    except (EOFError, OSError):
        return default_accounts

    if not manual_input:
        return default_accounts

    accounts = [item.strip() for item in re.split(r"[,\s]+", manual_input) if item.strip()]
    return accounts or default_accounts


if __name__ == "__main__":
    # Accounts to parse (handle, did, @handle, or bsky profile URL)
    ACCOUNTS_TO_PARSE = [
        "https://bsky.app/profile/jay.bsky.team",
        # "@handle.bsky.social",
        # "did:plc:...",
    ]
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!НАСТРОЙКИ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Loading options
    MAX_PAGES = 3
    MAX_POSTS = 0
    MIN_DATE = None
    MAX_FOLLOWERS = 10  # ЛИМИТЫ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!0 = без лимита
    MAX_FOLLOWS = 10  # ЛИМИТЫ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!0 = без лимита
    PRIOR = 1
    SYNC_ACCOUNT_COUNTERS = True
    SYNC_SOCIAL_GRAPH = False
    SYNC_FOLLOWS_GRAPH = False
    ENRICH_SOCIAL_PROFILES = True
    SYNC_POST_COMMENTERS = True
    COMMENT_THREAD_DEPTH = 16

    if 1:  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ЗАМЕНИТЬ НА 0 и 1 ПРИ НЕОБХОДИМОСТИ, ГДЕ 0 - НЕТ, 1 - ДА!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        SYNC_SOCIAL_GRAPH = True
    if 1:  # Включить/выключить синхронизацию подписок (кого читает аккаунт)
        SYNC_FOLLOWS_GRAPH = True

    # Example:
    # MIN_DATE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

    if False and sys.stdin and sys.stdin.isatty():
        try:
            manual_input = input(
                "Введите профиль(и) для парсинга (handle/@handle/did/url, через запятую). "
                "Enter = использовать список по умолчанию: "
            ).strip()
            if manual_input:
                ACCOUNTS_TO_PARSE = [x.strip() for x in re.split(r"[,\s]+", manual_input) if x.strip()]
        except EOFError:
            pass

    ACCOUNTS_TO_PARSE = prompt_accounts(ACCOUNTS_TO_PARSE)

    config = Config(
        ACCOUNTS=ACCOUNTS_TO_PARSE,
        MAX_PAGES=MAX_PAGES,
        MAX_POSTS=MAX_POSTS,
        MIN_DATE=MIN_DATE,
        MAX_FOLLOWERS=MAX_FOLLOWERS,
        MAX_FOLLOWS=MAX_FOLLOWS,
        PRIOR=PRIOR,
        SYNC_ACCOUNT_COUNTERS=SYNC_ACCOUNT_COUNTERS,
        SYNC_SOCIAL_GRAPH=SYNC_SOCIAL_GRAPH,
        SYNC_FOLLOWS_GRAPH=SYNC_FOLLOWS_GRAPH,
        ENRICH_SOCIAL_PROFILES=ENRICH_SOCIAL_PROFILES,
        SYNC_POST_COMMENTERS=SYNC_POST_COMMENTERS,
        COMMENT_THREAD_DEPTH=COMMENT_THREAD_DEPTH,
    )

    parser = BlueskyParser(config)
    try:
        parser.run()
    finally:
        parser.close()
