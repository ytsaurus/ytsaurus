#include "stdafx.h"
#include "codec.h"
#include "helpers.h"

#include <core/misc/protobuf_helpers.h>

#include <core/compression/helpers.pb.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> CompressWithEnvelope(
    const TSharedRef& uncompressedData,
    ECodec codecId)
{
    return CompressWithEnvelope(
        std::vector<TSharedRef>(1, uncompressedData),
        codecId);
}

std::vector<TSharedRef> CompressWithEnvelope(
    const std::vector<TSharedRef>& uncompressedData,
    ECodec codecId)
{
    NProto::TCompressedEnvelope envelope;
    if (codecId != ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    std::vector<TSharedRef> compressedData(2);
    YCHECK(SerializeToProto(envelope, &compressedData[0]));
    auto* codec = GetCodec(codecId);
    compressedData[1] = codec->Compress(uncompressedData);
    return compressedData;
}

TSharedRef DecompressWithEnvelope(const std::vector<TSharedRef>& compressedData)
{
    YCHECK(compressedData.size() == 2);

    NProto::TCompressedEnvelope envelope;
    YCHECK(DeserializeFromProto(&envelope, compressedData[0]));

    auto* codec = GetCodec(ECodec(envelope.codec()));
    return codec->Decompress(compressedData[1]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

