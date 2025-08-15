PY3TEST()

PEERDIR(
    contrib/python/asgi-lifespan
    contrib/python/fastapi
    contrib/python/pydantic/pydantic-2
    contrib/python/httpx
    contrib/python/sse-starlette
    contrib/python/pytest-asyncio
    contrib/python/uvicorn
)

NO_LINT()

PY_SRCS(
    NAMESPACE tests
    anyio_compat.py
)

TEST_SRCS(
    conftest.py
    test_event.py
    test_sse.py
)

END()
