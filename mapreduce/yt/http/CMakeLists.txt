LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    error.cpp
    http.cpp
    requests.cpp
    transaction.cpp
)

PEERDIR(
    library/string_utils/base64
    library/http/io
    mapreduce/yt/interface
    mapreduce/yt/common
)

END()
