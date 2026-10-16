LIBRARY()

SRCS(
    base_value_traits.cpp
    generic_hook.cpp
    get_traits.cpp
    hashtable.cpp
    node_holder.cpp
    options.cpp
    pointer_cast_traits.cpp
    size_tracker.cpp
    unordered_set.cpp
)

PEERDIR(
    library/cpp/threading/hazard_pointer/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
