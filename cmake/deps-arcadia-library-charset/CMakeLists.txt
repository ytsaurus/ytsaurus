set(NAME deps-arcadia-library-charset)
set(BASE ${CMAKE_SOURCE_DIR}/library/charset)
set(SRCS
  ${BASE}/ci_string.cpp
  ${BASE}/codepage.cpp
  ${BASE}/cp_encrec.cpp
  ${BASE}/doccodes.cpp
  ${BASE}/generated/cp_data.cpp
  ${BASE}/generated/encrec_data.cpp
  ${BASE}/iconv.cpp
  ${BASE}/wide.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-util
)

