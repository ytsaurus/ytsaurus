OWNER(
    vartyukh
)

PACKAGE()

END()

ADD_TEST(
    PY_TEST
        test.py
    DEPENDS
        mapreduce/yt/python
    TIMEOUT 100
)
