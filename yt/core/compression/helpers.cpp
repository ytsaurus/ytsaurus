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
    ECodec codecId,
    i64 maxPartSize)
{
    return CompressWithEnvelope(
        std::vector<TSharedRef>(1, uncompressedData),
        codecId,
        maxPartSize);
}

std::vector<TSharedRef> CompressWithEnvelope(
    const std::vector<TSharedRef>& uncompressedData,
    ECodec codecId,
    i64 maxPartSize)
{
    auto* codec = GetCodec(codecId);
    auto body = codec->Compress(uncompressedData);
    auto splinteredBody = body.Split(maxPartSize);

    NProto::TCompressedEnvelope envelope;
    if (codecId != ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
        envelope.set_part_count(static_cast<int>(splinteredBody.size()));
    }

    auto header = SerializeToProto(envelope);
    auto result = std::vector<TSharedRef>{header};
    result.insert(result.end(), splinteredBody.begin(), splinteredBody.end());
    return result;
}

TSharedRef DecompressWithEnvelope(const std::vector<TSharedRef>& compressedData)
{
    NProto::TCompressedEnvelope envelope;
    DeserializeFromProto(&envelope, compressedData[0]);

    YCHECK(envelope.part_count() > 0 && compressedData.size() == envelope.part_count() + 1);

    auto data = std::vector<TSharedRef>(compressedData.begin() + 1, compressedData.end());
    auto* codec = GetCodec(ECodec(envelope.codec()));

    return codec->Decompress(data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

