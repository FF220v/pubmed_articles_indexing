import json
from functools import lru_cache

import redis3


cached_funcs_results = dict()


def save_to_redis(session: redis3.Redis, data: dict) -> bool:
    return session.mset(json.dumps(data))


def get_from_redis(session: redis3.Redis, keys: list):
    return json.loads(session.mget(keys))


@lru_cache(maxsize=0)
def get_redis_config():
    with open("redis_config.json") as f:
        return json.load(f)


def get_redis_host():
    return get_redis_config()["host"]


def get_redis_port():
    return get_redis_config()["port"]


def get_redis_session(db):
    return redis3.Redis(host=get_redis_host(), port=get_redis_port(), db=db)


DOCS_DATABASE = 4  # includes minor info on each document such as title and url
KEYWORDS_INDEX = 0  # includes weighted descriptors, qualifiers, keywords
ABSTRACTS_INDEX = 1  # based on words from abstracts
TITLES_INDEX = 2  # based on words from titles
CHEMICALS_INDEX = 3  # based on chemical lists
AUTHOR_INDEX = 5  # based on associated author names
DATABASES_TO_PROCESS = (ABSTRACTS_INDEX, KEYWORDS_INDEX, TITLES_INDEX,
                        CHEMICALS_INDEX, AUTHOR_INDEX, DOCS_DATABASE)
INDICES_TO_PROCESS = (ABSTRACTS_INDEX, KEYWORDS_INDEX, TITLES_INDEX,
                      CHEMICALS_INDEX, AUTHOR_INDEX)
SESSIONS = {
    KEYWORDS_INDEX: get_redis_session(KEYWORDS_INDEX),
    ABSTRACTS_INDEX: get_redis_session(ABSTRACTS_INDEX),
    TITLES_INDEX: get_redis_session(TITLES_INDEX),
    CHEMICALS_INDEX: get_redis_session(CHEMICALS_INDEX),
    AUTHOR_INDEX: get_redis_session(AUTHOR_INDEX),
    DOCS_DATABASE: get_redis_session(DOCS_DATABASE)
}