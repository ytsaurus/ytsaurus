# - Find Perl
# This module finds if Perl is installed and determines where the
# executables are. This code sets the following variables:
#  
#  PERL:                path to the Perl compiler
#
# This code defines the following macros:
#
#  BUILDWITH_PERL(script destfile sourcefiles)
#

IF (WIN32)
	SET_IF_NOTSET(PERL		perl.exe)

	# TODO Write windows-code to get perl version
ELSE (WIN32)
	SET_IF_NOTSET(PERL		perl)
ENDIF (WIN32)

MACRO(IS_PERL_OK __res_)
    IF (NOT DEFINED IS_PERL_OK_RESULT)
        EXECUTE_PROCESS(OUTPUT_VARIABLE __tmp_
            COMMAND ${PERL} -V
            RESULT_VARIABLE __perl_exec_res_
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        IF (__perl_exec_res_ EQUAL 0)
            IS_VALID_PERL(__is_valid_perl_res_ __tmp_)
            IF (__is_valid_perl_res_) 
                ENABLE(IS_PERL_OK_RESULT)
            ELSE (__is_valid_perl_res_)
                DISABLE(IS_PERL_OK_RESULT)
                MESSAGE("Warning: Arcadia build needs Perl compiled with -DMULTIPLICITY")
            ENDIF (__is_valid_perl_res_)
        ELSE (__perl_exec_res_ EQUAL 0)
            DISABLE(IS_PERL_OK_RESULT)
            IF (${ARGC} GREATER 1)
                MESSAGE("${ARGN}")
            ENDIF (${ARGC} GREATER 1)
        ENDIF (__perl_exec_res_ EQUAL 0)
        SET(IS_PERL_OK_RESULT ${IS_PERL_OK_RESULT} CACHE BOOL "IS_PERL_OK result")
    ENDIF (NOT DEFINED IS_PERL_OK_RESULT)
    SET(${__res_} ${IS_PERL_OK_RESULT})
ENDMACRO(IS_PERL_OK)

MACRO(IS_VALID_PERL __res1_ __perloutputvar_)
    STRING(REGEX REPLACE "\n" " " __perloutput_ "${${__perloutputvar_}}")
    STRING(REGEX MATCH "Compile-time options(.*)MULTIPLICITY" __re_result_ "${__perloutput_}")
    IF (__re_result_) 
        ENABLE(${__res1_})
    ELSE (__re_result_)
        DISABLE(${__res1_})
    ENDIF (__re_result_)
ENDMACRO(IS_VALID_PERL)

MACRO(CHECK_PERL_VARS)
    IF (PERLXS OR PERLXSCPP OR USE_PERL) # || make(paths)
        IS_PERL_OK(__perl_exec_res_)
        IF (__perl_exec_res_)
            IF (NOT DEFINED PERLLIB)
                EXECUTE_PROCESS(OUTPUT_VARIABLE PERLLIB
                    COMMAND ${PERL} -V:archlib
                    RESULT_VARIABLE __perl_exec_res_
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                )
                STRING(REGEX REPLACE "archlib='(.*)';" "\\1" PERLLIB "${PERLLIB}")
                FILE(TO_CMAKE_PATH ${PERLLIB} PERLLIB)
            ENDIF (NOT DEFINED PERLLIB)
            SET_APPEND(DTMK_I ${PERLLIB}/CORE)

            IF (NOT DEFINED PERLLIB_PATH)
                SET(PERLLIB_PATH ${PERLLIB}/CORE)
                IF (NOT EXISTS ${PERLLIB_PATH}/libperl.a)
#                       SET(PERLLIB_PATH /usr/lib)
#                       ENDIF (EXISTS /usr/lib/libperl.a)
                ENDIF (NOT EXISTS ${PERLLIB_PATH}/libperl.a)
                SET(PERLLIB_PATH ${PERLLIB_PATH} CACHE STRING "PERLLIB_PATH")
            ENDIF (NOT DEFINED PERLLIB_PATH)

            IF (NOT DEFINED EXTUT)
                EXECUTE_PROCESS(OUTPUT_VARIABLE EXTUT
                    COMMAND ${PERL} -V:privlibexp
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                )
                STRING(REGEX REPLACE "privlibexp='(.*)';" "\\1" EXTUT "${EXTUT}")
                SET(EXTUT "${EXTUT}/ExtUtils")
                FILE(TO_CMAKE_PATH ${EXTUT} EXTUT)
            ENDIF (NOT DEFINED EXTUT)

            IF (NOT DEFINED PERLSITEARCHEXP)
                EXECUTE_PROCESS(OUTPUT_VARIABLE PERLSITEARCHEXP
                    COMMAND ${PERL} -V:sitearchexp
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                )
                STRING(REGEX REPLACE "sitearchexp='(.*)';" "\\1" PERLSITEARCHEXP "${PERLSITEARCHEXP}")
                FILE(TO_CMAKE_PATH ${PERLSITEARCHEXP} PERLSITEARCHEXP)
            ENDIF (NOT DEFINED PERLSITEARCHEXP)

            IF (NOT DEFINED PERLINSTALLSITEARCH)
                EXECUTE_PROCESS(OUTPUT_VARIABLE PERLINSTALLSITEARCH
                    COMMAND ${PERL} -V:installsitearch
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                )
                STRING(REGEX REPLACE "installsitearch='(.*)';" "\\1" PERLINSTALLSITEARCH "${PERLINSTALLSITEARCH}")
                FILE(TO_CMAKE_PATH "${PERLINSTALLSITEARCH}" PERLINSTALLSITEARCH)
                SET(PERLINSTALLSITEARCH "${PERLINSTALLSITEARCH}" CACHE STRING "perl -V:installsitearch")
            ENDIF (NOT DEFINED PERLINSTALLSITEARCH)
    
            IF (DEFINED USE_PERL_5_6)
                SET_APPEND(DTMK_D -DPERL_POLLUTE)
            ENDIF (DEFINED USE_PERL_5_6)
        ELSE (__perl_exec_res_)
            DEBUGMESSAGE(1 "FindPerl2.cmake: perl executable is not found")
        ENDIF (__perl_exec_res_)
    ENDIF (PERLXS OR PERLXSCPP OR USE_PERL) # || make(paths)
ENDMACRO(CHECK_PERL_VARS)

CHECK_PERL_VARS()


#
# BUILDWITH_PERL script dstfile commandargs
#   Tip: place NODEPEND keyword before an argument to remove it from DEPENDS section 
#        (used for non-srcfile perlscript arguments)
#
MACRO (BUILDWITH_PERL script dstfile commandargs)
    CHECK_PERL_VARS()
    
    IS_PERL_OK(__perl_exec_res_)
    IF (NOT __perl_exec_res_)
        MESSAGE(SEND_ERROR "BUILDWITH_PERL macro: perl not found")
    ENDIF (NOT __perl_exec_res_)
    
    SET(__cmdargs ${commandargs} ${ARGN})
    SET(__srcfiles)
    SET(__depfiles)
    SET(__next_nodepend "no")
    SET(__next_depend "no")
    FOREACH(__item_ ${__cmdargs})
	    IF (__next_nodepend)
		    SET_APPEND(__srcfiles ${__item_})
		    SET(__next_nodepend "no")
	    ELSEIF (__next_depend)
		    SET_APPEND(__depfiles ${__item_})
		    SET(__next_depend "no")
	    ELSE (__next_nodepend)
		    IF ("${__item_}" MATCHES "NODEPEND")
			    SET(__next_nodepend "yes")
		    ELSEIF ("${__item_}" MATCHES "DEPEND")
			    SET(__next_depend "yes")
		    ELSE ("${__item_}" MATCHES "NODEPEND")
			    SET_APPEND(__srcfiles ${__item_})
			    SET_APPEND(__depfiles ${__item_})
		    ENDIF ("${__item_}" MATCHES "NODEPEND")
	    ENDIF (__next_nodepend)
    ENDFOREACH(__item_)
    #SEPARATE_ARGUMENTS_SPACE(__srcfiles)
    #MESSAGE("__srcfiles=[${__srcfiles}], __depfiles=[${__depfiles}]")
    ADD_CUSTOM_COMMAND(
	    OUTPUT "${dstfile}"
	    PRE_BUILD
	    COMMAND ${PERL} ${script} ${__srcfiles} > ${dstfile} || ${RM} ${RM_FORCEFLAG} ${dstfile}
	    MAIN_DEPENDENCY "${script}"
	    DEPENDS ${script} ${__depfiles}
	    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	    COMMENT "Building ${dstfile} with perl"
	)
    SOURCE_GROUP("Custom Builds" FILES ${script})
    SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_PERL)

#
#
#
MACRO (BUILDWITH_PERLXSCPP srcfile dstfile)
    ENABLE(PERLXSCPP)
    CHECK_PERL_VARS()

    IS_PERL_OK(__perl_exec_res_)
    IF (NOT __perl_exec_res_)
        MESSAGE(SEND_ERROR "BUILDWITH_PERLXSCPP macro: perl not found")
    ENDIF (NOT __perl_exec_res_)

#	IF (PERLXSCPP)
		ADD_CUSTOM_COMMAND(
			OUTPUT ${dstfile}
			PRE_BUILD
			COMMAND ${PERL} ${EXTUT}/xsubpp -typemap ${EXTUT}/typemap -csuffix .cpp ${XSUBPPFLAGS} ${srcfile} > ${dstfile} || ${RM} ${RM_FORCEFLAG} ${dstfile}
			WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
			MAIN_DEPENDENCY "${srcfile}"
			DEPENDS ${srcfile} ${ARGN}
			COMMENT "Building ${dstfile} with perl (.xs.cpp)"
			)
#	ENDIF (PERLXSCPP)
	SOURCE_GROUP("Custom Builds" FILES ${srcfile})
	SOURCE_GROUP("Generated" FILES ${dstfile})
    IF (NOT WIN32)
        SET_SOURCE_FILES_PROPERTIES(${dstfile} PROPERTIES
            COMPILE_FLAGS -Wno-unused
        )
    ENDIF (NOT WIN32)
ENDMACRO (BUILDWITH_PERLXSCPP)

MACRO (BUILDWITH_PERLXS srcfile dstfile)
    ENABLE(PERLXS)
    CHECK_PERL_VARS()

    IS_PERL_OK(__perl_exec_res_)
    IF (NOT __perl_exec_res_)
        MESSAGE(SEND_ERROR "BUILDWITH_PERLXS macro: perl not found")
    ENDIF (NOT __perl_exec_res_)
    
#	IF (PERLXS)
		ADD_CUSTOM_COMMAND(
			OUTPUT ${dstfile}
			PRE_BUILD
			COMMAND ${PERL} ${EXTUT}/xsubpp -typemap ${EXTUT}/typemap ${XSUBPPFLAGS} ${srcfile} > ${dstfile} || ${RM} ${RM_FORCEFLAG} ${dstfile}
			WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
			MAIN_DEPENDENCY "${srcfile}"
			DEPENDS ${srcfile} ${ARGN}
			COMMENT "Building ${dstfile} with perl (.xs.c)"
			)
#	ENDIF (PERLXS)
	SOURCE_GROUP("Custom Builds" FILES ${srcfile})
	SOURCE_GROUP("Generated" FILES ${dstfile})
ENDMACRO (BUILDWITH_PERLXS)

