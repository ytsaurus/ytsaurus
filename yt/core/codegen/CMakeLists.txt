include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

llvm_map_components_to_libraries(
  REQUIRED_LLVM_LIBRARIES
  mcjit ipo x86 mcdisassembler linker asmparser irreader instrumentation
)

# We link statically with zlib, and LLVM (sometimes) tries to bring its own
# dependency.
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-lz")

add_library(yt-core-codegen ${YT_SRCS_RESOLVED})

target_link_libraries(yt-core-codegen
  ${REQUIRED_LLVM_LIBRARIES}
  yt-core
)
