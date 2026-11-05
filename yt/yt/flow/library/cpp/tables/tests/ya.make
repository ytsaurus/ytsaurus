RECURSE(
    test_input_messages
    test_key_states
    test_partition_states
    test_transaction_manager
)

IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    RECURSE(
        test_timer_pages
    )
ENDIF()
