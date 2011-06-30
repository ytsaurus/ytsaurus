#
# GetVariablesFromCMakeDashboards.cmake
#
# This is a CMake "-P script" to poll today's CMake dashboards for variable
# names and values. It gathers all the CMake variables used on all the CMake
# dashboards that have reported in today.
#
# Depends on:
#  cmake, of course (version 2.4 or later, for EXECUTE_PROCESS)
#  curl
#
# When a CMake dashboard is run, there is usually a SystemInformation test
# that runs. Part of the output of that test is to list all of the names and
# values of all of the CMake variables in use at the time the test is run.
# This CMake script uses regular expressions to find the start and end of the
# "AllVariables.txt" section of the SystemInformation test output and
# prints the results out in the form of XML.
#
#
# The CMake MESSAGE command prints to stderr, so you have to use a technique
# like this to get the output of this script as an XML file:
#
#   cmake -P GetVariablesFromCMakeDashboards.cmake > CMakeVars.xml 2>&1
#
#
# Once you have this XML file, it is a trivial matter to open it with Excel as
# an "XML List" and interact with it to see the scope and breadth of CMake variables
# that are defined/used on all the dashboard machines for the SystemInformation
# test.
#
# Feel free to apply other XML analysis tools and techniques to this data as well.
# I imagine that one of the things this output will be used for is the starting point
# (the complete list of all predefined CMake variables everywhere) for documenting
# "predefined" CMake variables.
#
#
# The procedure used to obtain this information looks like this:
#
# (1) Pull the contents of:
#     http://www.cmake.org/Testing/Dashboard/MostRecentResults-Nightly/TestOverviewByCount.html
#
# (2) Analyze the results of (1) to get today's nightly dashboard stamp so we can pull something like:
#     http://www.cmake.org/Testing/Dashboard/20060912-0100-Nightly/TestDetail/__Source_SystemInformation.html
#
# (3) For each line in the table in the results of (2) where the test has status "Passed",
#     pull the SystemInformation output for that build/site/buildstamp... and print all
#     the variables found there as XML output.
#
CMAKE_MINIMUM_REQUIRED(VERSION 2.4 FATAL_ERROR)


IF(NOT DEFINED AD_CURL_CMD)
  FIND_PROGRAM(AD_CURL_CMD "curl"
    PATHS "C:/cygwin/bin"
    )
ENDIF(NOT DEFINED AD_CURL_CMD)
IF(NOT AD_CURL_CMD)
  MESSAGE(FATAL_ERROR "ERROR: curl is required to run this script...")
ENDIF(NOT AD_CURL_CMD)


IF(NOT DEFINED AD_BASE_URL)
  SET(AD_BASE_URL "http://www.cmake.org/Testing")
ENDIF(NOT DEFINED AD_BASE_URL)

IF(NOT DEFINED AD_MOSTRECENT_URL)
  SET(AD_MOSTRECENT_URL "${AD_BASE_URL}/Dashboard/MostRecentResults-Nightly")
ENDIF(NOT DEFINED AD_MOSTRECENT_URL)

IF(NOT DEFINED AD_OVERVIEW_URL)
  SET(AD_OVERVIEW_URL "${AD_MOSTRECENT_URL}/TestOverviewByCount.html")
ENDIF(NOT DEFINED AD_OVERVIEW_URL)


# Construct today's nightly AD_DATESTAMP from AD_OVERVIEW_URL's redirect instruction:
#
EXECUTE_PROCESS(
  COMMAND "${AD_CURL_CMD}" "${AD_OVERVIEW_URL}"
  RESULT_VARIABLE AD_CURL_RV
  ERROR_VARIABLE AD_CURL_EV
  OUTPUT_VARIABLE AD_CURL_OV
  )
IF(NOT "${AD_CURL_RV}" STREQUAL "0")
  MESSAGE(FATAL_ERROR "ERROR: curl failed... AD_OVERVIEW_URL='${AD_OVERVIEW_URL}' AD_CURL_RV='${AD_CURL_RV}' AD_CURL_EV='${AD_CURL_EV}'")
ENDIF(NOT "${AD_CURL_RV}" STREQUAL "0")

STRING(REGEX REPLACE "^.*META HTTP-EQUIV=.Refresh.*URL=(.*)'>.*$" "\\1" AD_RELATIVE_URL ${AD_CURL_OV})

STRING(REGEX REPLACE "^../([^/]+)/.*$" "\\1" AD_DATESTAMP "${AD_RELATIVE_URL}")


# Get the list of dashboards to poll for SystemInformation test results:
#
SET(AD_SYSINFO_LIST "${AD_BASE_URL}/Dashboard/${AD_DATESTAMP}/TestDetail/__Source_SystemInformation.html")
EXECUTE_PROCESS(
  COMMAND "${AD_CURL_CMD}" "${AD_SYSINFO_LIST}"
  RESULT_VARIABLE AD_CURL_RV
  ERROR_VARIABLE AD_CURL_EV
  OUTPUT_VARIABLE AD_CURL_OV
  )
IF(NOT "${AD_CURL_RV}" STREQUAL "0")
  MESSAGE(FATAL_ERROR "ERROR: curl failed... AD_SYSINFO_LIST='${AD_SYSINFO_LIST}' AD_CURL_RV='${AD_CURL_RV}' AD_CURL_EV='${AD_CURL_EV}'")
ENDIF(NOT "${AD_CURL_RV}" STREQUAL "0")

# Convert output into a CMake list (where each element is one line of the output):
#
STRING(REGEX REPLACE ";" "\\\\;" AD_CURL_OV "${AD_CURL_OV}")
STRING(REGEX REPLACE "\n" ";" AD_CURL_OV "${AD_CURL_OV}")

# Gather the list of dashboard builds where the SystemInformation test passed:
#
FOREACH(item ${AD_CURL_OV})
  IF("${item}" MATCHES ".*<td class=.pass.><a href=..*/Sites/([^/]+)/([^/]+)/([^/]+)/.*.>.*")
    SET(AD_DASHBOARDS ${AD_DASHBOARDS} ${item})
  ENDIF("${item}" MATCHES ".*<td class=.pass.><a href=..*/Sites/([^/]+)/([^/]+)/([^/]+)/.*.>.*")
ENDFOREACH(item)


# Output opening XML:
#
MESSAGE("<Dashboards>")
#MESSAGE("<Dashboards")
#MESSAGE("  AD_CURL_CMD='${AD_CURL_CMD}'")
#MESSAGE("  AD_BASE_URL='${AD_BASE_URL}'")
#MESSAGE("  AD_MOSTRECENT_URL='${AD_MOSTRECENT_URL}'")
#MESSAGE("  AD_OVERVIEW_URL='${AD_OVERVIEW_URL}'")
#MESSAGE("  AD_RELATIVE_URL='${AD_RELATIVE_URL}'")
#MESSAGE("  AD_DATESTAMP='${AD_DATESTAMP}'")
#MESSAGE(">")


# For each dashboard, try to get the list of CMake variables
# spewed by the SystemInformation test...
#
FOREACH(item ${AD_DASHBOARDS})
  STRING(REGEX REPLACE ".*<td class=.pass.><a href=..*/(Sites/.*SystemInformation.html).>.*" "\\1" AD_PARTIAL_URL "${item}")
  SET(AD_SYSINFOTEST_URL "${AD_BASE_URL}/${AD_PARTIAL_URL}")

  STRING(REGEX REPLACE ".*<td class=.pass.><a href=..*/Sites/([^/]+)/([^/]+)/([^/]+)/.*.>.*" "\\1" AD_SITE_NAME "${item}")
  STRING(REGEX REPLACE ".*<td class=.pass.><a href=..*/Sites/([^/]+)/([^/]+)/([^/]+)/.*.>.*" "\\2" AD_BUILD_NAME "${item}")
  STRING(REGEX REPLACE ".*<td class=.pass.><a href=..*/Sites/([^/]+)/([^/]+)/([^/]+)/.*.>.*" "\\3" AD_BUILD_STAMP "${item}")

  MESSAGE(" <Dashboard site='${AD_SITE_NAME}' build='${AD_BUILD_NAME}' buildstamp='${AD_BUILD_STAMP}'>")

  EXECUTE_PROCESS(
    COMMAND "${AD_CURL_CMD}" "${AD_SYSINFOTEST_URL}"
    RESULT_VARIABLE AD_CURL_RV
    ERROR_VARIABLE AD_CURL_EV
    OUTPUT_VARIABLE AD_CURL_OV
    )
  IF(NOT "${AD_CURL_RV}" STREQUAL "0")
    MESSAGE("  <ERROR message='curl failed... AD_SYSINFOTEST_URL=${AD_SYSINFOTEST_URL} AD_CURL_RV=${AD_CURL_RV} AD_CURL_EV=${AD_CURL_EV}'/>")
  ELSE(NOT "${AD_CURL_RV}" STREQUAL "0")

    # Convert output into a CMake list (where each element is one line of the output):
    #
    STRING(REGEX REPLACE ";" "\\\\;" AD_CURL_OV "${AD_CURL_OV}")
    STRING(REGEX REPLACE "\n" ";" AD_CURL_OV "${AD_CURL_OV}")

    SET(inscope_variables 0)
    FOREACH(vitem ${AD_CURL_OV})
      IF("${vitem}" MATCHES "^Contents of .*AllVariables.txt.:$")
        SET(inscope_variables 1)
      ENDIF("${vitem}" MATCHES "^Contents of .*AllVariables.txt.:$")

      IF("${inscope_variables}" STREQUAL "1")
        IF("${vitem}" MATCHES "^Avoid ctest truncation of output")
          SET(inscope_variables 0)
        ELSE("${vitem}" MATCHES "^Avoid ctest truncation of output")
          IF("${vitem}" MATCHES "^([^ ]+) \\\"(.*)\\\"$")
            STRING(REGEX REPLACE "^([^ ]+) \\\"(.*)\\\"$" "\\1" AD_VAR_NAME "${vitem}")
            STRING(REGEX REPLACE "^([^ ]+) \\\"(.*)\\\"$" "\\2" AD_VAR_VALUE "${vitem}")

            # Since we use '' as xml attribute delimiter:
            STRING(REGEX REPLACE "'" "&apos;" AD_VAR_VALUE "${AD_VAR_VALUE}")

            MESSAGE("  <Variable name='${AD_VAR_NAME}' value='${AD_VAR_VALUE}'/>")
          ENDIF("${vitem}" MATCHES "^([^ ]+) \\\"(.*)\\\"$")
        ENDIF("${vitem}" MATCHES "^Avoid ctest truncation of output")
      ENDIF("${inscope_variables}" STREQUAL "1")
    ENDFOREACH(vitem)
  ENDIF(NOT "${AD_CURL_RV}" STREQUAL "0")

  MESSAGE(" </Dashboard>")
ENDFOREACH(item)


# Output closing XML:
#
MESSAGE("</Dashboards>")
