GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.0.0-20250102033503-faa5f7b0171c)

SRCS(
    constants.go
    context.go
    csi_entry_state.go
    csi_param_state.go
    escape_intermediate_state.go
    escape_state.go
    event_handler.go
    ground_state.go
    osc_string_state.go
    parser.go
    parser_action_helpers.go
    parser_actions.go
    states.go
    utilities.go
)

GO_TEST_SRCS(
    parser_test.go
    parser_test_helpers_test.go
    parser_test_utilities_test.go
    test_event_handler_test.go
)

END()

RECURSE(
    gotest
)

IF (OS_WINDOWS)
    RECURSE(
        winterm
    )
ENDIF()
