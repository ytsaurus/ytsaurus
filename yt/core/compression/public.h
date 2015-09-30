#pragma once

#include <core/misc/public.h>
#include <core/misc/string.h>

#include <unordered_map>

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
    // TODO(acid): Replace this with yhash_map when brace initialization is supported.
    static const std::unordered_map<Stroka, Stroka> renamedCodecs = {
        {"GzipNormal", "Zlib6"},
        {"GzipBestCompression", "Zlib9"}};

    Stroka decodedValue = DecodeEnumValue(value);
    if (renamedCodecs.find(decodedValue) != renamedCodecs.end()) {
        decodedValue = renamedCodecs.at(decodedValue);
    }

    return TEnumTraits<NCompression::ECodec>::FromString(decodedValue);
}

} // namespace NYT
