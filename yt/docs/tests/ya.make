PY3TEST()

SUBSCRIBER(
    andozer
    arivkin
    ogorbacheva
)

PEERDIR(
    contrib/python/PyYAML
)

TEST_SRCS(
    test_toc_audience_layering.py
)

DATA(
    arcadia/yt/docs/ru
)

END()
