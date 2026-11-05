# Feature flags for YT projects.
# Include in the global scope: variables set inside a module are invisible to RECURSE and other
# global-scope commands.
IF (NOT OPENSOURCE)
    SET(YT_ENABLE_SEQUOIA 1)
    SET(YT_ENABLE_JOURNAL_NBD 1)
ENDIF()
