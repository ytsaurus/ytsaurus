cmake_minimum_required(VERSION 2.8)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_SOURCE_DIR}/python/yt/packages/requests
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_SOURCE_DIR}/yt/packages/requests
)

file(
  COPY ${CMAKE_SOURCE_DIR}/contrib/python-requests/requests
  DESTINATION ${CMAKE_SOURCE_DIR}/yt/packages/
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_SOURCE_DIR}/python/yt/packages/argcomplete
)

file(
  COPY ${CMAKE_SOURCE_DIR}/contrib/python-argcomplete/argcomplete
  DESTINATION ${CMAKE_SOURCE_DIR}/yt/packages/
)
