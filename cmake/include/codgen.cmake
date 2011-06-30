# - Find gperf and flex
# This module finds if gperf and flex are installed and determines where the
# executables are. This code sets the following variables:
#
#  LEX:                  path to the flex compiler
#  GPERF:                GPERF code generator
#  GP_FLAGS:             GPERF translator flags
#
# This code defines the following macros:
#
#  BUILDWITH_GPERF(sourcefile destfile)
#  BUILDWITH_LEX(sourcefile destfile)
#

# GET_FILENAME_COMPONENT(LEX_INCLUDE_DIR ${LEX} PATH) # sourcename?
SET(LEX_INCLUDE_DIR ${ARCADIA_ROOT}/contrib/tools/flex-old)

SET(LEX_FLAGS)

MACRO (BUILDWITH_LEX srcfile dstfile) # dstfile usually has .l extension
    TOOLDIR_EX(
        contrib/tools/flex-old LEX
    )

    IF (NOT LEX)
        MESSAGE(SEND_ERROR "LEX is not defined. Please check if contrib/tools/flex-old is checked out")
    ENDIF (NOT LEX)
    ADD_CUSTOM_COMMAND(
       OUTPUT ${dstfile}
       COMMAND ${LEX} ${LEX_FLAGS} -o${dstfile} ${srcfile}
       MAIN_DEPENDENCY "${srcfile}"
       DEPENDS ${srcfile} flex ${ARGN}
       COMMENT "Building \"${dstfile}\" with lex"
       WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
    INCLUDE_DIRECTORIES(${LEX_INCLUDE_DIR})
ENDMACRO (BUILDWITH_LEX)

# flags for all cases
# -a         # ansi-prototypes (old version)
# -L ANSI-C  # ansi-prototypes (new version)
# -C         # const-strings
# -p         # return-pointer  (old version) ???
# -t         # user-type
# -T         # dont-print-type-definiton-use-h-file-instead

# usually we have a lot of keys
# -k*        # all-bytes-to-try
# -D         # disambiguate
# -S1        # generate-one-switch (m.b. not neccessary ?)

SET_APPEND(GP_FLAGS -CtTLANSI-C)

# usually we match string prefix to a key
# i.e. html-entities, word-preffixes are NOT nul-terminated
# -c         # use-strncmp

IF (NOT DEFINED GPERF_FEW_KEYS)
    SET_APPEND(GP_FLAGS -Dk*)
ENDIF (NOT DEFINED GPERF_FEW_KEYS)

IF (NOT DEFINED GPERF_FULL_STRING_MATCH)
    SET_APPEND(GP_FLAGS -c)
ENDIF (NOT DEFINED GPERF_FULL_STRING_MATCH)

MACRO (BUILDWITH_GPERF srcfile dstfile) # dstfile usually has .gperf.cpp extension
    IF (DEFINED GPERF_LOOKUP_FN)
        SET(LOCAL_GPERF_LOOKUP_FN ${GPERF_LOOKUP_FN})
    ELSE (DEFINED GPERF_LOOKUP_FN)
        GET_FILENAME_COMPONENT(LOCAL_GPERF_LOOKUP_FN ${dstfile} NAME_WE)
        SET(LOCAL_GPERF_LOOKUP_FN in_${LOCAL_GPERF_LOOKUP_FN}_set)
    ENDIF (DEFINED GPERF_LOOKUP_FN)

    TOOLDIR_EX(
        contrib/tools/gperf GPERF
    )

    IF (NOT GPERF)
        MESSAGE(SEND_ERROR "GPERF is not defined. Please check if contrib/tools/gperf is checked out")
    ENDIF (NOT GPERF)

    ADD_CUSTOM_COMMAND(
        OUTPUT ${dstfile}
        PRE_BUILD
        COMMAND ${GPERF} ${GP_FLAGS} -N${LOCAL_GPERF_LOOKUP_FN} ${srcfile} >${dstfile} || \(${RM} ${RM_FORCEFLAG} ${dstfile} && exit 1\)
        MAIN_DEPENDENCY "${srcfile}"
        DEPENDS ${srcfile} gperf ${ARGN}
        COMMENT "Building ${dstfile} with gperf"
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_GPERF)

DEFAULT(YASM_OBJECT_FORMAT elf)
DEFAULT(YASM_PLATFORM "UNIX")

IF (DARWIN)
    SET(YASM_OBJECT_FORMAT macho)
    SET(YASM_PLATFORM "DARWIN")
ENDIF (DARWIN)

IF (WIN32)
    SET(YASM_OBJECT_FORMAT win)
    SET(YASM_PLATFORM "WIN")
ENDIF (WIN32)

MACRO (BUILDWITH_YASM srcfile dstfile)
    TOOLDIR_EX(
        contrib/tools/yasm YASM
    )

    IF (NOT YASM)
        MESSAGE(SEND_ERROR "YASM is not defined. Please check if contrib/tools/yasm is checked out")
    ENDIF (NOT YASM)
    ADD_CUSTOM_COMMAND(
        OUTPUT "${dstfile}"
        COMMAND "${YASM}" -f "${YASM_OBJECT_FORMAT}${HARDWARE_ARCH}" -D "${YASM_PLATFORM}" -D "_${HARDWARE_TYPE}_" -o "${dstfile}" "${srcfile}"
        COMMENT "Building \"${dstfile}\" with yasm"
        MAIN_DEPENDENCY "${srcfile}"
        DEPENDS "${srcfile}" yasm ${ARGN}
        WORKING_DIRECTORY ${BINDIR}
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_YASM)

TOOLDIR_EX(
    contrib/tools/byacc BYACC
)

DEFAULT(BYACC_FLAGS -vdo)

MACRO (BUILDWITH_BYACC srcfile dstfile)
    IF (NOT BYACC)
        MESSAGE(SEND_ERROR "BYACC is not defined. Please check if contrib/tools/byacc is checked out")
    ENDIF (NOT BYACC)
        GET_FILENAME_COMPONENT(LOCAL_BYACC_DSTFILE_FILE ${dstfile} NAME_WE)
        GET_FILENAME_COMPONENT(LOCAL_BYACC_DSTFILE_PATH ${dstfile} PATH)
        SET(LOCAL_BYACC_DSTFILE_H "${LOCAL_BYACC_DSTFILE_PATH}/${LOCAL_BYACC_DSTFILE_FILE}.h")
    ADD_CUSTOM_COMMAND(
        OUTPUT ${dstfile} ${LOCAL_BYACC_DSTFILE_H}
        COMMAND "${BYACC}" ${BYACC_FLAGS} "${dstfile}" "${srcfile}"
        MAIN_DEPENDENCY "${srcfile}"
        DEPENDS "${srcfile}" byacc ${ARGN}
        COMMENT "Building \"${dstfile}\"+\"${LOCAL_BYACC_DSTFILE_H}\" with byacc"
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_BYACC)

TOOLDIR_EX(
    contrib/tools/ragel5/ragel __ragel5_
    contrib/tools/ragel5/rlgen-cd __rlgen_cd_
)

SET(RAGEL ${__ragel5_})
SET(RLGEN ${__rlgen_cd_})
FILE(TO_NATIVE_PATH "${RLGEN}" RLGEN)

DEFAULT(RAGEL_FLAGS "")
DEFAULT(RLGEN_FLAGS -G2)

MACRO (BUILDWITH_RAGEL srcfile dstfile)
    IF (NOT RAGEL OR NOT RLGEN)
        MESSAGE(SEND_ERROR "RAGEL or RLGEN is not defined. Please check if contrib/tools/ragel5 is checked out")
    ENDIF (NOT RAGEL OR NOT RLGEN)
    GET_FILENAME_COMPONENT(LOCAL_RAGEL_SRCFILE_PATH ${srcfile} PATH)
    ADD_CUSTOM_COMMAND(
        OUTPUT "${dstfile}"
        COMMAND "${RAGEL}" ${RAGEL_FLAGS} "${srcfile}" | "${RLGEN}" ${RLGEN_FLAGS} -o "${dstfile}"
        COMMENT "Building \"${dstfile}\" with ragel"
        MAIN_DEPENDENCY "${srcfile}"
        DEPENDS "${srcfile}" ragel5 rlgen-cd ${ARGN}
        WORKING_DIRECTORY ${LOCAL_RAGEL_SRCFILE_PATH}
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_RAGEL)

# This macro generates source files with structure descriptions
# for field_calc.
#
#  BUILDWITH_STRUCT2FIELDCALC(STRUCTS HEADERS OUTFILE)
#
# STRUCTS - zero or more structures (like "struct1;struct2;...) to generate parser for
# HEADERS - full paths to one or more headers (like "${ARCADIA_ROOT}/xxx/yyy/header.h;...")
# OUTFILE - path to resulting generated file
#

MACRO (BUILDWITH_STRUCT2FIELDCALC STRUCTS HEADERS OUTFILE)
    TOOLDIR_EX(
        tools/struct2fieldcalc __struct2fieldcalc_
    )
    SET(STRUCT2FIELDCALC_ARGS)
    FOREACH(struct ${STRUCTS})
        SET(STRUCT2FIELDCALC_ARGS ${STRUCT2FIELDCALC_ARGS} -s ${struct})
    ENDFOREACH(struct)
    FOREACH(enum ${ENUMS})
        SET(STRUCT2FIELDCALC_ARGS ${STRUCT2FIELDCALC_ARGS} -e ${enum})
    ENDFOREACH(enum)
    ADD_CUSTOM_COMMAND(
        OUTPUT  ${OUTFILE}
        COMMAND ${__struct2fieldcalc_} ${STRUCT2FIELDCALC_ARGS} ${HEADERS} > ${OUTFILE} || ${RM} ${RM_FORCEFLAG} ${OUTFILE}
        DEPENDS ${HEADERS} struct2fieldcalc ${ARGN}
        COMMENT "Building \"${OUTFILE}\" with struct2fieldcalc"
    )
ENDMACRO (BUILDWITH_STRUCT2FIELDCALC)

# This macro generates source files from .proto files
#
#  BUILDWITH_PROTOC(srcfile var_dst_cc var_dst_hh)

MACRO (BUILDWITH_PROTOC srcfile var_dst_cc var_dst_hh)
    GET_PROTO_RELATIVE(${srcfile} __want_relative)
    BUILDWITH_PROTOC_IMPL(${srcfile} ${var_dst_cc} ${var_dst_hh} "${__want_relative}" ${ARGN})
ENDMACRO (BUILDWITH_PROTOC)


FUNCTION (GET_RELATIVE_TO_TOP srcfile rel_var top_var)

    # determine the top of the hierarchy where srcfile
    # is located and its name relative to that top;
    # we only consider ARCADIA_ROOT and CMAKE_BINARY_DIR

    FOREACH (top ${CMAKE_BINARY_DIR} ${ARCADIA_ROOT})

        # see if srcfile is under ${top}
        FILE(RELATIVE_PATH rel ${top} ${srcfile})
        IF (NOT IS_ABSOLUTE ${rel} AND NOT rel MATCHES "^\\.\\./")
            SET(${top_var} ${top} PARENT_SCOPE)
            SET(${rel_var} ${rel} PARENT_SCOPE)
            return()
        ENDIF()

    ENDFOREACH ()

    # don't know where it is, so don't use relative and don't modify top_var
    SET(${rel_var} ${srcfile} PARENT_SCOPE)

ENDFUNCTION ()


# This function generates source files from .proto files optionally using the
# path of the .proto file relative to either source or build root, as opposed
# to the file portion of the path name.
#
# If the path specified to protoc is relative, it will generate the code in a
# way that is consistent with importing the .proto using the same relative path
# rather than the file portion.
#
# For instance:
# * this can only be accessed via import "bar/baz.proto"
# $ protoc ... bar/baz.proto
# * this can only be accessed via import "baz.proto"
# $ protoc ... /foo/bar/baz.proto
#
#  BUILDWITH_PROTOC_IMPL(srcfile var_dst_cc var_dst_hh want_relative)

FUNCTION (BUILDWITH_PROTOC_IMPL srcfile var_dst_cc var_dst_hh want_relative)

    # contains the protoc
    SET(protoc_dir contrib/tools/protoc)
    TOOLDIR_EX(${protoc_dir} __protoc_)
    TOOLDIR_EX(${protoc_dir}/plugins/cpp_styleguide __cpp_styleguide_)

    # determine the working dir and relative path for srcfile
    SET(src_proto_dir "")
    GET_FILENAME_COMPONENT(src_proto_path ${srcfile} ABSOLUTE)
    IF (want_relative)
        GET_RELATIVE_TO_TOP(${src_proto_path} src_proto_rel src_proto_dir)
    ELSE ()
        SET(src_proto_rel ${src_proto_path})
    ENDIF ()

    # set the destination directory
    IF (src_proto_dir STREQUAL "")
        GET_FILENAME_COMPONENT(src_proto_dir ${src_proto_rel} PATH)
        GET_FILENAME_COMPONENT(src_proto_rel ${src_proto_rel} NAME)
        SET(dst_proto_dir ${BINDIR})
    ELSE ()
        SET(dst_proto_dir ${CMAKE_BINARY_DIR})
    ENDIF ()
    SET(dst_proto_path "${dst_proto_dir}/${src_proto_rel}")

    # determine the files which protoc will create; protoc removes .protodevel
    # and .proto extensions, if present, then appends .pb.{h,cc} for output
    STRING(REGEX REPLACE "\\.protodevel$" "" dst_proto_pref "${dst_proto_path}")
    IF (dst_proto_pref STREQUAL dst_proto_path)
        STRING(REGEX REPLACE "\\.proto$" "" dst_proto_pref "${dst_proto_path}")
    ENDIF ()
    SET(dst_proto_hh "${dst_proto_pref}.pb.h")
    SET(dst_proto_cc "${dst_proto_pref}.pb.cc")

    # return the name of generated source files to the caller
    SET(${var_dst_cc} ${dst_proto_cc} PARENT_SCOPE)
    SET(${var_dst_hh} ${dst_proto_hh} PARENT_SCOPE)

    GET_SOURCE_FILE_PROPERTY(__bwpc_flags_ "${srcfile}" CODGENFLAGS)
    IF (NOT __bwpc_flags_)
        SET(__bwpc_flags_)
    ELSE (NOT __bwpc_flags_)
        DEBUGMESSAGE(2 "BUILDWITH_PROTOC got flags for ${srcfile}: [${__bwpc_flags_}]")
    ENDIF (NOT __bwpc_flags_)

    IF (UNIX)
        SET(__cmd_mkdir mkdir -p ${dst_proto_dir})
    ELSE ()
        FILE(TO_NATIVE_PATH ${dst_proto_dir} __native)
        SET(__cmd_mkdir if not exist \"${__native}\" mkdir \"${__native}\")
    ENDIF ()
    ADD_CUSTOM_COMMAND(
        OUTPUT            ${dst_proto_cc} ${dst_proto_hh}
        COMMAND           ${__cmd_mkdir}
        COMMAND           ${__protoc_}
            -I=. -I=${ARCADIA_ROOT} -I=${CMAKE_BINARY_DIR}
            --cpp_out=${dst_proto_dir}
            --cpp_styleguide_out=${dst_proto_dir}
            --plugin=protoc-gen-cpp_styleguide=${__cpp_styleguide_}
            ${__bwpc_flags_}
            ${src_proto_rel}
        MAIN_DEPENDENCY   ${src_proto_path}
        DEPENDS           protoc cpp_styleguide ${ARGN}
        WORKING_DIRECTORY ${src_proto_dir}
        COMMENT           "Generating C++ source from ${srcfile}"
    )

    SET_DIR_HAS_GENERATED()

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated"     FILES ${dst_proto_cc} ${dst_proto_hh})

ENDFUNCTION (BUILDWITH_PROTOC_IMPL)


MACRO (SRCS_PROTO_REL)
    FOREACH(srcfile ${ARGN})
        SRCS(${srcfile} PROTO_RELATIVE yes)
    ENDFOREACH ()
ENDMACRO ()


MACRO (BUILDWITH_PROTOC_REL srcfile var_dst_cc var_dst_hh)
    BUILDWITH_PROTOC_IMPL(${srcfile} ${var_dst_cc} ${var_dst_hh} TRUE ${ARGN})
ENDMACRO ()


MACRO (SET_PROTO_RELATIVE)
    SET_SOURCE_FILES_PROPERTIES(${ARGN} PROPERTIES PROTO_RELATIVE TRUE)
ENDMACRO ()

MACRO (SET_PROTO_RELATIVE_DEFAULT val)
    SET(PROTO_RELATIVE_DEFAULT ${val})
ENDMACRO ()

MACRO (GET_PROTO_RELATIVE srcfile var)
    GET_SOURCE_FILE_PROPERTY(${var} ${srcfile} PROTO_RELATIVE)
    IF (${var} STREQUAL "NOTFOUND")
        SET(${var} "${PROTO_RELATIVE_DEFAULT}")
    ENDIF ()
ENDMACRO ()


MACRO (BUILDWITH_PROTOC_EVENTS srcfile dstfile)
    TOOLDIR_EX(contrib/tools/protoc __protoc_)
    TOOLDIR_EX(contrib/tools/protoc/plugins/cpp_styleguide __cpp_styleguide_)
    TOOLDIR_EX(tools/event2cpp __proto_events_)

    GET_FILENAME_COMPONENT(LOCAL_PROTOC_DSTFILE ${dstfile} NAME_WE)
    GET_FILENAME_COMPONENT(LOCAL_PROTOC_DSTPATH ${dstfile} PATH)
    GET_FILENAME_COMPONENT(SRC_PROTOC_PATH ${srcfile} PATH)
    GET_FILENAME_COMPONENT(SRC_PROTOC_NAME ${srcfile} NAME)

    SET(LOCAL_PROTOC_DSTFILE_H "${LOCAL_PROTOC_DSTPATH}/${SRC_PROTOC_NAME}.pb.h")

    GET_SOURCE_FILE_PROPERTY(__bwpc_flags_ "${srcfile}" CODGENFLAGS)
    IF (NOT __bwpc_flags_)
        SET(__bwpc_flags_)
    ELSE (NOT __bwpc_flags_)
        DEBUGMESSAGE(2 "BUILDWITH_PROTOC_EVENTS got flags for ${srcfile}: [${__bwpc_flags_}]")
    ENDIF (NOT __bwpc_flags_)

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}
            ${LOCAL_PROTOC_DSTFILE_H}
        COMMAND
            ${__protoc_} "--cpp_out=${BINDIR}"
            "--plugin=protoc-gen-cpp_styleguide=${__cpp_styleguide_}"
            "--cpp_styleguide_out=${BINDIR}"
            "--plugin=protoc-gen-event2cpp=${__proto_events_}"
            "--event2cpp_out=${BINDIR}"
            "-I."
            "-I${ARCADIA_ROOT}"
            "-I${ARCADIA_ROOT}/library/eventlog"
            ${__bwpc_flags_} "${SRC_PROTOC_NAME}"
        MAIN_DEPENDENCY
            "${srcfile}"
        WORKING_DIRECTORY
            ${SRC_PROTOC_PATH}
        DEPENDS
            protoc cpp_styleguide event2cpp "${srcfile}" ${ARGN}
        COMMENT
            "Events ${srcfile}: Building ${dstfile} and ${LOCAL_PROTOC_DSTFILE_H}"
    )
    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated"     FILES ${dstfile})
ENDMACRO (BUILDWITH_PROTOC_EVENTS)

# transform XML -> whatever using XSLT
#
# BUILDWITH_XSLTPROC(srcfile xslfile dstfile)

MACRO (BUILDWITH_XSLTPROC srcfile xslfile dstfile)
    TOOLDIR_EX(
        contrib/tools/xsltproc __xsltproc_
    )
    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}
        COMMAND
            ${__xsltproc_} -o ${dstfile} ${XSLTPROC_FLAGS} ${xslfile} ${srcfile}
        DEPENDS
            xsltproc ${srcfile} ${xslfile} ${ARGN}
        COMMENT
            "Building ${dstfile} with xsltproc"
    )
ENDMACRO (BUILDWITH_XSLTPROC)

# C++ -> XML compilation using gccxml
#
# BUILDWITH_GCCXML(srcfile dstfile)

MACRO (BUILDWITH_GCCXML srcfile dstfile)
    ADD_SUBDIRECTORY_EX(${ARCADIA_ROOT}/contrib/tools/gccxml ${ARCADIA_BUILD_ROOT}/contrib/tools/gccxml)
    GET_TARGET_PROPERTY(__gccxml_cc1plus_ gccxml_cc1plus LOCATION)
    GET_TARGET_PROPERTY(__gccxml_ gccxml LOCATION)

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}
        COMMAND
            ${__gccxml_} ${COMMON_CXXFLAGS} -I ${ARCADIA_ROOT} -I ${ARCADIA_BUILD_ROOT} -I ${ARCADIA_ROOT}/util/private/stl/stlport-${CURRENT_STLPORT_VERSION} --gccxml-compiler ${CMAKE_CXX_COMPILER} --gccxml-root ${ARCADIA_ROOT}/contrib/tools/gccxml/GCC_XML/Support --gccxml-executable ${__gccxml_cc1plus_} --gccxml-cpp ${__gccxml_cc1plus_} -fxml=${dstfile} ${srcfile}
        DEPENDS
            ${srcfile} gccxml_cc1plus gccxml ${ARGN}
        COMMENT
            "Building ${dstfile} with gccxml"
    )
ENDMACRO (BUILDWITH_GCCXML)

# DO NOT use thic macro in your projects. Use PEERDIR(library/svnversion) instead and
# include <library/svnversion/svnversion.h> file to define PROGRAM_VERSION macro.
MACRO (CREATE_SVNVERSION_FOR srcfile)
    TOOLDIR_EX(
        tools/lua lua
    )

    IF (IGNORE_SVNVERSION)
        CFLAGS(-DIGNORE_SVNVERSION)
    ELSE (IGNORE_SVNVERSION)
        IF (EXISTS "${ARCADIA_ROOT}/.svn/entries")
            SET(SVN_DEPENDS "${ARCADIA_ROOT}/.svn/entries")
        ELSEIF (EXISTS "${ARCADIA_ROOT}/.git")
            SET(SVN_DEPENDS "${ARCADIA_ROOT}/.git")
        ENDIF(EXISTS "${ARCADIA_ROOT}/.svn/entries")
    ENDIF (IGNORE_SVNVERSION)

    SET(__svd_ "${BINDIR}/svnversion_data.h")
    ADD_CUSTOM_COMMAND(
        OUTPUT
            "${__svd_}"

        COMMAND
            ${lua} ${ARCADIA_ROOT}/cmake/include/svn_info.lua ${ARCADIA_ROOT}/.svn/entries ${CMAKE_CXX_COMPILER} ${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${REALPRJNAME}.dir/flags.make ${ARCADIA_BUILD_ROOT} > "${__svd_}"

        DEPENDS
            ${ARCADIA_ROOT}/cmake/include/svn_info.lua
            ${SVN_DEPENDS}
            lua

        WORKING_DIRECTORY
            ${CMAKE_CURRENT_BINARY_DIR}

        COMMENT
            "Generating ${__svd_}"

        VERBATIM
    )

    SET_SOURCE_FILES_PROPERTIES(srcfile PROPERTIES OBJECT_DEPENDS "${__svd_}")

    SRCS(
        "${__svd_}"
    )
ENDMACRO (CREATE_SVNVERSION_FOR)

# This macro generates source files from .asp files
#
#  BUILDWITH_HTML2CPP(srcfile dstfile)

MACRO (BUILDWITH_HTML2CPP srcfile dstfile)
    TOOLDIR_EX(
        tools/html2cpp __html2cpp_
    )

    GET_FILENAME_COMPONENT(SRC_HTML2CPP_PATH ${srcfile} PATH)
    GET_FILENAME_COMPONENT(SRC_HTML2CPP_NAME ${srcfile} NAME)

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}
        COMMAND
            ${__html2cpp_} ${srcfile} ${dstfile}
        MAIN_DEPENDENCY
            "${srcfile}"
        WORKING_DIRECTORY
            ${SRC_HTML2CPP_PATH}
        DEPENDS
            html2cpp "${srcfile}" ${ARGN}
        COMMENT
            "Building ${dstfile} with html2cpp"
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated"     FILES ${dstfile})
ENDMACRO (BUILDWITH_HTML2CPP)

# This macro generates source files from .fml files
#
# BUILD_FORMULA(srcfile dstfile)

MACRO (BUILD_FORMULA srcfile dstfile)
    TOOLDIR_EX(
        tools/relev_fml_codegen __relev_fml_codegen_
    )

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}

        COMMAND
            ${__relev_fml_codegen_}
                -b -o ${dstfile}
                 < ${srcfile}
                || ${RM} ${dstfile}

        MAIN_DEPENDENCY ${srcfile}

        DEPENDS
            ${srcfile}
            relev_fml_codegen

        COMMENT
            "Building ${dstfile}"
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})

    SRCS(${dstfile})
ENDMACRO (BUILD_FORMULA)

# This macro generates source files from .fml2 files
#
# BUILD_FORMULA_2(srcfile dstfile dstfile2)

MACRO (BUILD_FORMULA_2 srcfile dstfile dstfile2)
    TOOLDIR_EX(
        tools/relev_fml_codegen __relev_fml_codegen_
    )

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile} ${dstfile2}

        COMMAND
            ${__relev_fml_codegen_}
                -b -o ${dstfile} -2 -O ${dstfile2}
                 < ${srcfile}
                || ${RM} ${dstfile} ${dstfile2}

        MAIN_DEPENDENCY ${srcfile}

        DEPENDS
            ${srcfile}
            relev_fml_codegen

        COMMENT
            "Building ${dstfile} ${dstfile2}"
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile} ${dstfile2})

    SRCS(${dstfile})
    SRCS(${dstfile2})
ENDMACRO (BUILD_FORMULA_2)

# This macro generates source files from .fml3 files
#
# BUILD_FORMULA_3(srcfile dstfile dstfile2)

MACRO (BUILD_FORMULA_3 srcfile srcpath dstfile output2 prefix)
    TOOLDIR_EX(
        tools/relev_fml_codegen __relev_fml_codegen_
    )

    ADD_CUSTOM_COMMAND(
        OUTPUT
            ${dstfile}

        COMMAND
            ${__relev_fml_codegen_}
                -b -o ${dstfile} -3 -O ${output2} -L ${srcpath}/${prefix}.list -P ${prefix}
                 < ${srcfile}
                || ${RM} ${dstfile}

        MAIN_DEPENDENCY ${srcfile}

        DEPENDS
            ${srcfile}
            relev_fml_codegen
            ${srcpath}/${prefix}.list
        
        COMMENT
            "Building ${dstfile} ${prefix}.*.cpp"
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})

    SRCS(${dstfile})
ENDMACRO (BUILD_FORMULA_3)

MACRO (BUILD_MN mname)
    TOOLDIR_EX(quality/relev_tools/mx_ops __mx_ops_)
    SET(cmddep)

    SET(srcfile "matrixnet.${mname}.info")
    SET(dstpath "${cwd}/../matrixnetformulas")
    SET(dstfname "matrixnet.${mname}.cpp")
    SET(dstfile "${dstpath}/${dstfname}")

    SET(target ${dstfile}.factors)
    ADD_CUSTOM_COMMAND(
        # gmake might remove the primary target if command fails
        # since .factors is less important, use it instead of .cpp
        OUTPUT ${target}
        WORKING_DIRECTORY ${cwd}
        COMMAND touch ${dstfile}
        COMMAND ${__mx_ops_} codegen -f ${srcfile} ${dstfile} ${mname}
        DEPENDS ${srcfile} ${__mx_ops_} ${cmddep}
        COMMENT "Generating matrixnet ${mname}"
    )

    FILE(RELATIVE_PATH dstincl ${cwd}/${mxdir} ${dstpath})
    ADD_CUSTOM_COMMAND(
        # don't advertise the actual files we're writing to
        OUTPUT ${cwd}/${mxfuncs}
        # cmake escapes spaces inside a string

        COMMAND echo "extern" "const" "TMXNetInfoStatic" "'staticMn${mname};'" >> ${mxfuncs}.1
        COMMAND echo ${dstfname} >> ${mxfiles}.1
        DEPENDS ${target}
        COMMENT "Adding ${dstfile} to the build"
        APPEND
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})

ENDMACRO (BUILD_MN)

# Generates .inc from .sfdl
MACRO (BUILD_SFDL srcfile dstfile deps)
    TOOLDIR_EX(tools/calcstaticopt __calcstaticopt)

    FILE(TO_NATIVE_PATH ${srcfile} __srcfile_native)
    FILE(TO_NATIVE_PATH ${dstfile} __dstfile_native)

    GET_FILENAME_COMPONENT(__dstfile_dir ${__dstfile_native} PATH)
    GET_FILENAME_COMPONENT(__dstfile_namewe ${__dstfile_native} NAME_WE)
    SET(__tmpfile ${__dstfile_dir}/${__dstfile_namewe}.i)

    IF (WIN32)
        SET(__cpp_flags /E /C)
    ELSE (WIN32)
        SET(__cpp_flags $(CXX_DEFINES) $(CXX_FLAGS) -E -C -x c++)
    ENDIF (WIN32)

    ADD_CUSTOM_COMMAND(
        OUTPUT ${dstfile}
        COMMAND ${RM} ${RM_FORCEFLAG} ${__dstfile_native}.tmp ${__dstfile_native}
        COMMAND ${CMAKE_CXX_COMPILER} ${__cpp_flags} ${__srcfile_native} >${__tmpfile}
        COMMAND ${__calcstaticopt} -i ${__tmpfile} -a ${ARCADIA_ROOT} >${__dstfile_native}.tmp
        COMMAND ${MV} ${MV_FORCEFLAG} ${__dstfile_native}.tmp ${__dstfile_native}
        MAIN_DEPENDENCY ${srcfile}
        DEPENDS ${srcfile} ${__calcstaticopt} ${deps}
        COMMENT "Generating ${dstfile}"
    )

    SOURCE_GROUP("Custom Builds" FILES ${srcfile})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILD_SFDL)
