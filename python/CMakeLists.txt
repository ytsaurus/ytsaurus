foreach(package_name "prettytable")
  file(
    REMOVE_RECURSE
    ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages/${package_name}*
    ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages/${package_name}*.py
  )

  SET(PACKAGE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../contrib/python/${package_name})

  file(GLOB FILES_TO_COPY
    ${PACKAGE_DIR}/${package_name}*.py
  )

  SET(SOURCE_DIR_INSIDE_PACKAGE ${PACKAGE_DIR}/${package_name})
  if(EXISTS ${SOURCE_DIR_INSIDE_PACKAGE})
    SET(FILES_TO_COPY ${FILES_TO_COPY} ${SOURCE_DIR_INSIDE_PACKAGE})
  endif()

  file(
    COPY ${FILES_TO_COPY}
    DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/yp/packages
  )
endforeach(package_name)
