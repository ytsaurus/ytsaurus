exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_SOURCE_DIR}/python/yt/packages/requests
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_SOURCE_DIR}/contrib/libs/python-requests/requests
  ${CMAKE_SOURCE_DIR}/python/yt/packages/requests
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E remove
  ${CMAKE_SOURCE_DIR}/python/yt/packages/argcomplete
)

exec_program(
  ${CMAKE_COMMAND} ARGS -E create_symlink
  ${CMAKE_SOURCE_DIR}/contrib/libs/python-argcomplete/argcomplete
  ${CMAKE_SOURCE_DIR}/python/yt/packages/argcomplete
)
