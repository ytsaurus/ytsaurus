# Common defaults for YT C++ projects
INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (SANITIZER_TYPE == "address")
    IF (OS_LINUX)
        LDFLAGS(-Wl,--compress-debug-sections=zlib -gdwarf64 -mcmodel=large -gsplit-dwarf -mlarge-data-threshold=1)
        CFLAGS(-gdwarf64 -mcmodel=large -gsplit-dwarf -mlarge-data-threshold=1)
    ENDIF()
ENDIF()
