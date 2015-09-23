#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NCompression {

///////////////////////////////////////////////////////////////////////////////

struct ICodec;

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)                       (0))
    ((Snappy)                     (1))
    ((GzipNormal)                 (2))
    ((GzipBestCompression)        (3))
    ((Lz4)                        (4))
    ((Lz4HighCompression)         (5))
    ((QuickLz)                    (6))
    ((Zstd)                       (7))
    ((Brotli3)                    (8))
    ((Brotli5)                    (9))
    ((Brotli8)                   (10))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
