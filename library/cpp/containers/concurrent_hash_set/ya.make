LIBRARY()

SRCS(
    concurrent_hash_set.cpp
)

NO_COW()

PEERDIR(
    library/cpp/yt/threading
)

END()
