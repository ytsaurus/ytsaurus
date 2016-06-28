cmake_minimum_required(VERSION 2.8)

# This synchronizes different copies of CMake spawned by CLion.
file(
  LOCK ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/cmake.lock
  GUARD FILE
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/requests
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-requests/requests
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/argcomplete
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-argcomplete/argcomplete
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/pympler
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-pympler/pympler
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/cherrypy
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-cherrypy/cherrypy
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/tornado
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-tornado/tornado
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/certifi
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-certifi/certifi
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/backports_abc.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-backports_abc/backports_abc.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/backports
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-backports.ssl_match_hostname/backports
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/six.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-six/six.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/singledispatch.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-singledispatch/singledispatch.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)

file(
  REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/singledispatch_helpers.py
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-singledispatch/singledispatch_helpers.py
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages
)
