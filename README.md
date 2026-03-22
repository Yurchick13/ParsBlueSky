# BlueSky Parser

Парсер постов из социальной сети BlueSky для загрузки данных в графовую базу данных Neo4j.

## Описание

Скрипт загружает посты из указанных аккаунтов BlueSky и сохраняет их в Neo4j с графовой структурой:

- Аккаунты пользователей
- Посты с метаданными
- Текст постов (отдельным узлом)
- Ссылки и домены
- Хештеги
- Медиафайлы (фото, видео, внешние файлы)
- Связи между объектами (репосты, ответы, упоминания)

## Структура графа

```text
(BlueSky_Account)-[:POSTED]->(BlueSky_Post)
(BlueSky_Post)-[:MENTIONS]->(BlueSky_Account)
(BlueSky_Post)-[:TAGS]->(Hashtag)
(BlueSky_Post)-[:REBLOG_OF]->(BlueSky_Post)
(BlueSky_Post)-[:REPLY_TO]->(BlueSky_Post)
(Text)-[:part]->(BlueSky_Post)
(URI)-[:part]->(BlueSky_Post)
(Domain)-[:rel]->(URI)
(Photo|Video|File)-[:part]->(BlueSky_Post)
```

## Требования

- Python 3.8+
- Neo4j 4.x или 5.x
- Аккаунт BlueSky
- App Password BlueSky (не обычный пароль)

## Установка

1. Установите зависимости:

```bash
pip install -r requirements.txt
```

2. Убедитесь, что Neo4j запущен и доступен по адресу `bolt://localhost:7687`.

3. Создайте App Password в BlueSky:
   `Settings -> App Passwords -> Add App Password`

## Настройка

Рекомендуется передавать креды через переменные окружения.

### PowerShell (Windows)

```powershell
$env:NEO4J_URI="bolt://localhost:7687"
$env:NEO4J_USER="neo4j"
$env:NEO4J_PASSWORD="your_neo4j_password"

$env:BLUESKY_IDENTIFIER="your_handle.bsky.social"   # или email
$env:BLUESKY_PASSWORD="xxxx-xxxx-xxxx-xxxx"         # App Password
```

### Параметры загрузки в `main.py`

В блоке `if __name__ == "__main__":`:

- `ACCOUNTS_TO_PARSE` — список аккаунтов (handle, DID, `@handle`, URL профиля)
- `MAX_PAGES` — макс. страниц на аккаунт (`0` = без ограничений)
- `MAX_POSTS` — макс. постов на аккаунт (`0` = без ограничений)
- `MIN_DATE` — минимальная дата (`None` = без ограничений)
- `PRIOR` — приоритет аккаунтов в БД

Пример:

```python
ACCOUNTS_TO_PARSE = [
    "https://bsky.app/profile/jay.bsky.team",
    "https://bsky.app/profile/atproto.com",
]

MAX_PAGES = 3
MAX_POSTS = 0
MIN_DATE = None
PRIOR = 1
```

## Запуск

```bash
python main.py
```

или

```bash
python bluesky_pars.py
```

## Логирование

События пишутся:

- в консоль
- в файл `bluesky_pars.log`

Типы сообщений:

- `START` — начало операции
- `END` — завершение операции
- `success` — успешно
- `info` — информация
- `warning` — предупреждение
- `error` — ошибка
- `process` — процесс выполнения
- `data` — статистика

## Проверка на дубликаты

- Дубликаты не создаются (`MERGE` по id/uri)
- При повторном запуске данные обновляются
- Прогресс загрузки хранится в `BlueSky_Account.last_post_uri`

## Примеры Cypher-запросов

Посты аккаунта:

```cypher
MATCH (a:BlueSky_Account {handle: "jay.bsky.team"})-[:POSTED]->(p:BlueSky_Post)
RETURN p.id, p.created_at, p.like_count, p.repost_count
ORDER BY p.created_at DESC
```

Посты с хештегом:

```cypher
MATCH (p:BlueSky_Post)-[:TAGS]->(h:Hashtag {name: "python"})
RETURN p.id, p.created_at
ORDER BY p.created_at DESC
```

Связанные домены:

```cypher
MATCH (d:Domain)-[:rel]->(u:URI)<-[:part]-(p:BlueSky_Post)
RETURN d.name, count(u) AS uri_count, count(p) AS post_count
ORDER BY uri_count DESC
```

Статистика графа:

```cypher
MATCH (n)
RETURN labels(n)[0] AS label, count(n) AS count
ORDER BY count DESC
```

## Структура проекта

```text
BlueSkyPars/
├── main.py            # Основной скрипт парсера
├── bluesky_pars.py    # Альтернативный запуск парсера
├── requirements.txt   # Зависимости Python
├── README.md          # Документация
└── bluesky_pars.log   # Логи (создаётся при запуске)
```

## Возможные ошибки

### Neo4j auth failed / Unauthorized

- Проверьте `NEO4J_USER` и `NEO4J_PASSWORD`
- Убедитесь, что Neo4j запущен и доступен

### BlueSky auth failed / Invalid identifier or password

- Используйте App Password, а не обычный пароль
- Проверьте `BLUESKY_IDENTIFIER` (handle/email)

### Ограничения API / временные ошибки сети

- Скрипт делает повторный запрос после паузы
- При необходимости уменьшите `MAX_PAGES`

## Безопасность

- Не храните реальные пароли в коде
- Используйте переменные окружения или `.env`
- Если пароль случайно попал в репозиторий/чат, лучше сразу сменить его

## Лицензия

Проект предоставлен "как есть" для образовательных целей.
