LIBRARY()

SRCS(
    name_service.cpp
    namespacing.cpp
)

END()

RECURSE(
    fallback
    static
    union
)
