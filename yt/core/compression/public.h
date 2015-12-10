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
    ((Zlib6)                      (2))
    ((Zlib9)                      (3))
    ((Lz4)                        (4))
    ((Lz4HighCompression)         (5))
    ((QuickLz)                    (6))
    ((Zstd)                       (7))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

namespace NYT {

template <>
inline NCompression::ECodec ParseEnum(
    const Stroka& value,
    typename TEnumTraits<NCompression::ECodec>::TType*)
{
    Stroka decodedValue;
    if (value == "GzipNormal") {
        decodedValue = "Zlib6";
    } else if (value == "GzipBestCompression") {
        decodedValue = "Zlib9";
    } else {
        decodedValue = value;
    }

    return TEnumTraits<NCompression::ECodec>::FromString(decodedValue);
}

} // namespace NYT
