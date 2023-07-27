UNITTEST_FOR(contrib/libs/farmhash)

OWNER(
    g:cpp-contrib
)

NO_COMPILER_WARNINGS()

CFLAGS(-DFARMHASHSELFTEST)

SRCS(test.cc)

END()
