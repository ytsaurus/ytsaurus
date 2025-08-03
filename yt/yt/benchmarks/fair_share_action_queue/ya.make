PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
    old_two_level_fair_share_thread_pool.cpp
)

PEERDIR(
  yt/yt/core
  library/cpp/getopt
  yt/yt/library/signals
)

END()

