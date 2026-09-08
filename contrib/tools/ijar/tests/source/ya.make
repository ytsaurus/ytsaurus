PROGRAM(ijar)

WITHOUT_LICENSE_TEXTS()

NO_COMPILER_WARNINGS()

NO_UTIL()

ADDINCL(
    contrib/tools/ijar
)

CFLAGS(
    -DBLAZE_OPENSOURCE
)

PEERDIR(
    contrib/libs/zlib
)

SRCS(
    ../../src/main/cpp/util/file.cc
    ../../src/main/cpp/util/logging.cc
    ../../src/main/cpp/util/path.cc
    ../../src/main/cpp/util/port.cc
    ../../src/main/cpp/util/strings.cc
    ../../third_party/ijar/classfile.cc
    ../../third_party/ijar/ijar.cc
    ../../third_party/ijar/platform_utils.cc
    ../../third_party/ijar/zip.cc
    ../../third_party/ijar/zlib_client.cc
)

IF (OS_WINDOWS)
    SRCS(
        ../../src/main/cpp/util/errors_windows.cc
        ../../src/main/cpp/util/file_windows.cc
        ../../src/main/cpp/util/path_windows.cc
        ../../src/main/native/windows/file.cc
        ../../src/main/native/windows/util.cc
        ../../third_party/ijar/mapped_file_windows.cc
    )
ELSE()
    SRCS(
        ../../src/main/cpp/util/errors_posix.cc
        ../../src/main/cpp/util/file_posix.cc
        ../../src/main/cpp/util/path_posix.cc
        ../../third_party/ijar/mapped_file_unix.cc
    )
ENDIF()

END()
