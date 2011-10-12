LIBRARY(ytlib)

IF(CMAKE_COMPILER_IS_GNUXX)
    SET(CMAKE_CXX_FLAGS "-std=c++0x")
ENDIF(CMAKE_COMPILER_IS_GNUXX)

SET(HEADERS "")

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
        GET_FILENAME_COMPONENT(__file_ext_ ${__file_} EXT)

        IF ("X${__file_path_}X" STREQUAL "XX")
            SET(__file_path_ "${YT_SOURCE_DIRECTORY}")
        ENDIF ("X${__file_path_}X" STREQUAL "XX")

        IF ("${__file_ext_}" STREQUAL ".h")
            SET_APPEND(HEADERS "${__file_path_}/${__file_name_}")
        ENDIF ("${__file_ext_}" STREQUAL ".h")

        SET_APPEND(SRCS "${__file_path_}/${__file_name_}")
    ENDFOREACH(__file_)

    UNSET(YT_SOURCE_FILES)
    UNSET(YT_SOURCE_DIRECTORY)

    SOURCE_GROUP(${DIR} REGULAR_EXPRESSION ${DIR}/.*)
ENDMACRO(YT_SUBDIRECTORY)

YT_SUBDIRECTORY(actions)
#YT_SUBDIRECTORY(client)
YT_SUBDIRECTORY(election)
YT_SUBDIRECTORY(chunk_holder)
YT_SUBDIRECTORY(chunk_client)
YT_SUBDIRECTORY(chunk_manager)
YT_SUBDIRECTORY(transaction_manager)
YT_SUBDIRECTORY(logging)
YT_SUBDIRECTORY(meta_state)
YT_SUBDIRECTORY(misc)
YT_SUBDIRECTORY(rpc)
YT_SUBDIRECTORY(bus)
YT_SUBDIRECTORY(ytree)
YT_SUBDIRECTORY(cypress)
YT_SUBDIRECTORY(table_client)
YT_SUBDIRECTORY(monitoring)

# Removes header files from build in VS
SET_SOURCE_FILES_PROPERTIES(
    ${HEADERS}
    PROPERTIES HEADER_FILE_ONLY TRUE
)

PEERDIR(
    contrib/libs/protobuf
    quality/Misc
    quality/NetLiba
    dict/json
    library/json
    util/server
)

SET(USE_LF_ALLOCATOR yes)

END()
