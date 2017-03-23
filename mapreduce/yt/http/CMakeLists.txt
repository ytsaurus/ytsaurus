LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    abortable_http_response.cpp
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
