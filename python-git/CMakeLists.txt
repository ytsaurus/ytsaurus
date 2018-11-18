cmake_minimum_required(VERSION 2.8)

# This synchronizes different copies of CMake spawned by CLion.
if(NOT (${CMAKE_VERSION} VERSION_LESS 3.2.3))
  file(
    LOCK ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/cmake.lock
    GUARD FILE
  )
endif()

foreach(package_name "requests" "argcomplete" "certifi" "tornado"
                     "backports_abc" "six" "singledispatch" "dill" "distro"
                     "urllib3" "chardet" "idna")
  file(
    REMOVE_RECURSE
    ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/${package_name}
    ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/${package_name}*
    ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/${package_name}*.py
  )
  file(GLOB FILES_TO_COPY
    ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-${package_name}/${package_name}*.py
  )

  SET(SOURCE_PACKAGE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-${package_name}/${package_name})
  if(EXISTS ${SOURCE_PACKAGE_DIR})
    SET(FILES_TO_COPY ${FILES_TO_COPY} ${SOURCE_PACKAGE_DIR})
  endif()

  file(
    COPY ${FILES_TO_COPY}
    DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
  )
endforeach(package_name)

# Additinal packages to copy
file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/backports
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-backports.ssl_match_hostname/backports
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/decorator.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-decorator/src/decorator.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

# Prepare yt_yson_bindings
file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt_yson_bindings
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/yson-debian/yt_yson_bindings
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/fuse.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-fusepy/fuse.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)
