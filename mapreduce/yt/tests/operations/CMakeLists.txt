LIBRARY()

OWNER(monster)

SRCS(
    fail_map.cpp
    fail_map.h
    id_map.cpp
    id_map.h
    id_reduce.cpp
    id_reduce.h
)

PEERDIR(
    mapreduce/interface
)

END()
