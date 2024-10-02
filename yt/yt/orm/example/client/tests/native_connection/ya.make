RECURSE_FOR_TESTS(
    cluster
    main
    # Shutdown-related tests must run as a separate test.
    shutdown
)
