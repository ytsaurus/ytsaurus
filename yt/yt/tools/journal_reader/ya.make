PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
    journal_dumper.cpp
    mutation_dumper.cpp
)

PEERDIR(
  yt/yt/ytlib
  yt/yt/server/lib
  yt/yt/server/lib/hydra
  yt/yt/server/lib/hive
  yt/yt/server/lib/transaction_supervisor
  yt/yt/server/lib/tablet_node
  library/cpp/getopt/small
)

END()
