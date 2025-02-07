GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

DATA(
    arcadia/yt/yt/server/lib/squash_fs/unittests/canondata/1_empty_squash_fs
    arcadia/yt/yt/server/lib/squash_fs/unittests/canondata/2_simple_squash_fs
    arcadia/yt/yt/server/lib/squash_fs/unittests/canondata/3_big_File_squash_fs
    arcadia/yt/yt/server/lib/squash_fs/unittests/canondata/4_many_files_squash_fs
)

SRCS(
    squash_fs_ut.cpp
)

SIZE(LARGE)

TAG(
    ya:fat
    ya:force_sandbox
    ya:sandbox_coverage
)

# The container was build by this task:
# https://sandbox.yandex-team.ru/task/2527465793/view .
REQUIREMENTS(container:6957399703)

PEERDIR(
    yt/yt/server/lib/nbd
    yt/yt/core/test_framework
    yt/yt/client
    yt/yt/ytlib
)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
