cmake_minimum_required(VERSION 2.8)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_CURRENT_SOURCE_DIR}/python/yt/packages/requests
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/requests
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-requests/requests
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/argcomplete
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-argcomplete/argcomplete
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/pympler
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-pympler/pympler
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/cherrypy
)

file(
  COPY ${CMAKE_CURRENT_SOURCE_DIR}/contrib/python-cherrypy/cherrypy
  DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yt/packages/
)
