LIBRARY()

PEERDIR(
    library/cpp/threading/hazard_pointer/intrusive
    library/cpp/threading/hazard_pointer/options
    library/cpp/threading/hazard_pointer/structs
    library/cpp/threading/hazard_pointer/utils
)

SRCS (
    hazard_pointer.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    intrusive
    options
    structs
    utils
)
