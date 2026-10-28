GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.0.0-20250102033503-faa5f7b0171c)

IF (OS_WINDOWS)
    SRCS(
        ansi.go
        api.go
        attr_translation.go
        cursor_helpers.go
        erase_helpers.go
        scroll_helper.go
        utilities.go
        win_event_handler.go
    )
ENDIF()

END()
