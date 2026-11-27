GO_LIBRARY()

SRCS(
    acl_tree.go
    data_model.go
)

IF (OPENSOURCE)
    SRCS(
        data_model_external.go
    )
ELSE()
    SRCS(
        data_model_internal.go
    )
ENDIF()

END()
