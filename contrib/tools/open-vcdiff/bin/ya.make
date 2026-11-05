PROGRAM(vcdiff)

VERSION(2019-03-11)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

PEERDIR(
    contrib/libs/gflags
    contrib/tools/open-vcdiff
)

ADDINCL(
    contrib/tools/open-vcdiff/src
)

SRCDIR(contrib/tools/open-vcdiff/src)

SRCS(
    vcdiff_main.cc
)

END()
