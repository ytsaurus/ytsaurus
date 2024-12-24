LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    journal_as_local_file_read_only_changelog.cpp
    helpers.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
