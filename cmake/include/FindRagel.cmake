INCLUDE(${CMAKE_MODULE_PATH}/MakefileHelpers.cmake)

TOOLDIR_EX(
    contrib/tools/ragel6 __ragel6_
)

SET(RAGEL6 ${__ragel6_})

MACRO (BUILDWITH_RAGEL6 srcfile dstfile)
    ADD_CUSTOM_COMMAND(
	    OUTPUT "${dstfile}"
	    COMMAND "${RAGEL6}" ${RAGEL6_FLAGS} -o "${dstfile}" "${srcfile}"
	    MAIN_DEPENDENCY "${srcfile}"
	    DEPENDS "${srcfile}" ragel6 ${ARGN}
	    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	    COMMENT "Building \"${dstfile}\" with ragel6"
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_RAGEL6)

IF (WIN32)
    SET_IF_NOTSET(RAGEL6_FLAGS -mCG2)
ELSE (WIN32)
    SET_IF_NOTSET(RAGEL6_FLAGS -CG2)
ENDIF (WIN32)
