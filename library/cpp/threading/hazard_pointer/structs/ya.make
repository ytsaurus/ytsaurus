LIBRARY()

SRCS(
    active_list.cpp
    free_list.cpp
    thread_local_list.cpp
)

PEERDIR(
    library/cpp/threading/hazard_pointer/intrusive
    library/cpp/threading/hazard_pointer/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
