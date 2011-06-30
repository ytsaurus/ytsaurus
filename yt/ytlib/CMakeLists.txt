LIBRARY(ytlib)

MACRO(YT_SUBDIRECTORY DIR)
    SET(YT_SOURCE_FILES "")
    SET(YT_SOURCE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})

    # Alter ${SRCDIR} for included script.
    SET(SAVED_SRCDIR ${SRCDIR})
    SET(SRCDIR ${SRCDIR}/${DIR})
    INCLUDE(${DIR}/CMakeLists.txt)
    SET(SRCDIR ${SAVED_SRCDIR})

    # Plug defined source files in the main build.
    FOREACH(__file_ ${YT_SOURCE_FILES})
        GET_FILENAME_COMPONENT(__file_path_ ${__file_} PATH)
        GET_FILENAME_COMPONENT(__file_name_ ${__file_} NAME)

        IF ("X${__file_path_}X" STREQUAL "XX")
            SET(__file_path_ "${YT_SOURCE_DIRECTORY}")
        ENDIF ("X${__file_path_}X" STREQUAL "XX")

        SET_APPEND(SRCS "${__file_path_}/${__file_name_}")
    ENDFOREACH(__file_)

    UNSET(YT_SOURCE_FILES)
    UNSET(YT_SOURCE_DIRECTORY)

    SOURCE_GROUP(${DIR} REGULAR_EXPRESSION ${DIR}/.*)
ENDMACRO(YT_SUBDIRECTORY)

YT_SUBDIRECTORY(actions)
YT_SUBDIRECTORY(rpc)
YT_SUBDIRECTORY(misc)
YT_SUBDIRECTORY(client)
YT_SUBDIRECTORY(holder)
YT_SUBDIRECTORY(master)
YT_SUBDIRECTORY(logging)

PEERDIR(
    contrib/libs/protobuf
    quality/Misc
    quality/NetLiba
    dict/json
)

SET(USE_LF_ALLOCATOR yes)

END()
