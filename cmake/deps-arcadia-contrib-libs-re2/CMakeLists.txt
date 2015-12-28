set(NAME deps-arcadia-contrib-libs-re2)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/re2)
set(SRCS
  ${BASE}/generated/unicode_casefold.cc
  ${BASE}/generated/unicode_groups.cc
  ${BASE}/re2/bitstate.cc
  ${BASE}/re2/compile.cc
  ${BASE}/re2/dfa.cc
  ${BASE}/re2/filtered_re2.cc
  ${BASE}/re2/mimics_pcre.cc
  ${BASE}/re2/nfa.cc
  ${BASE}/re2/onepass.cc
  ${BASE}/re2/parse.cc
  ${BASE}/re2/perl_groups.cc
  ${BASE}/re2/prefilter.cc
  ${BASE}/re2/prefilter_tree.cc
  ${BASE}/re2/prog.cc
  ${BASE}/re2/re2.cc
  ${BASE}/re2/regexp.cc
  ${BASE}/re2/set.cc
  ${BASE}/re2/simplify.cc
  ${BASE}/re2/tostring.cc
  ${BASE}/stubs/output.cc
  ${BASE}/stubs/stringpiece.cc
  ${BASE}/util/hash.cc
  ${BASE}/util/rune.cc
  ${BASE}/util/stringprintf.cc
  ${BASE}/util/strutil.cc
  ${BASE}/util/valgrind.cc
)

include_directories(${CMAKE_SOURCE_DIR} ${BASE})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  ytext-arcadia-util
)
