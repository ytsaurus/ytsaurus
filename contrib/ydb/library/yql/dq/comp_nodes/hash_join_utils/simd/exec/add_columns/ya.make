IF (ARCH_X86_64 AND OS_LINUX)

PROGRAM(add_columns)

SRCS(main.cpp)

SIZE(MEDIUM)

CFLAGS(-mavx2)

PEERDIR(contrib/ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

END()

ENDIF()
