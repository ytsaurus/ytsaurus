OWNER(
    exprmntr
    vartyukh
)

PACKAGE()

END()

ADD_TEST(
    PY_TEST
        test.py
    TAG
        ya:external
    DEPENDS
        mapreduce/yt/python
    TIMEOUT 100
)
