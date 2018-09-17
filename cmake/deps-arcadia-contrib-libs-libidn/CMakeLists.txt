set(NAME deps-arcadia-contrib-libs-libidn)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/libidn)
set(SRCS
  ${BASE}/idn-free.c
  ${BASE}/idna.c
  ${BASE}/nfkc.c
  ${BASE}/pr29.c
  ${BASE}/profiles.c
  ${BASE}/punycode.c
  ${BASE}/rfc3454.c
  ${BASE}/strerror-idna.c
  ${BASE}/strerror-pr29.c
  ${BASE}/strerror-punycode.c
  ${BASE}/strerror-stringprep.c
  ${BASE}/strerror-tld.c
  ${BASE}/stringprep.c
  ${BASE}/tld.c
  ${BASE}/tlds.c
  ${BASE}/toutf8.c
  ${BASE}/version.c
  ${BASE}/strverscmp.c
  ${BASE}/striconv.c
  ${BASE}/c-strcasecmp.c
  ${BASE}/c-ctype.c
)

include_directories(${BASE})

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

set_source_files_properties(${SRCS} PROPERTIES COMPILE_FLAGS -DHAVE_CONFIG_H)

if (_is_gcc)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
