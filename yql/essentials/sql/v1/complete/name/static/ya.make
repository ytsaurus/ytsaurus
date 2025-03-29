LIBRARY()

SRCS(
    json_name_set.cpp
    name_service.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name
)

RESOURCE(
    yql/essentials/data/language/types.json types.json
    yql/essentials/data/language/sql_functions.json sql_functions.json
)

END()
