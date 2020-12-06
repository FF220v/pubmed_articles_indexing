from aiohttp import ClientSession


__session_cache = None


def get_client_session():
    global __session_cache
    if __session_cache is None:
        __session_cache = ClientSession()
    return __session_cache