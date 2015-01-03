#pragma once

#include <core/misc/common.h>

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
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
