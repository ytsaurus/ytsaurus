#pragma once

#include <yt/core/misc/public.h>
#include <yt/core/misc/string.h>

namespace NYT {
namespace NCompression {

///////////////////////////////////////////////////////////////////////////////

struct ICodec;

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)                       (0))
    ((Snappy)                     (1))
    ((Lz4)                        (4))
    ((Lz4HighCompression)         (5))
    ((QuickLz)                    (6))
    ((Zstd)                       (7))

    ((Brotli_1)                  (11))
    ((Brotli_2)                  (12))
    ((Brotli_3)                   (8))
    ((Brotli_4)                  (13))
    ((Brotli_5)                   (9))
    ((Brotli_6)                  (14))
    ((Brotli_7)                  (15))
    ((Brotli_8)                  (10))
    ((Brotli_9)                  (16))
    ((Brotli_10)                 (17))
    ((Brotli_11)                 (18))

    ((Zlib_1)                    (19))
    ((Zlib_2)                    (20))
    ((Zlib_3)                    (21))
    ((Zlib_4)                    (22))
    ((Zlib_5)                    (23))
    ((Zlib_6)                     (2))
    ((Zlib_7)                    (24))
    ((Zlib_8)                    (25))
    ((Zlib_9)                     (3))

    // Deprecated
    ((Zlib6)                      (2))
    ((GzipNormal)                 (2))
    ((Zlib9)                      (3))
    ((GzipBestCompression)        (3))
    ((Brotli3)                    (8))
    ((Brotli5)                    (9))
    ((Brotli8)                   (10))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
