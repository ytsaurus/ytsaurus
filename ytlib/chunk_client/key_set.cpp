#include "key_set.h"

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

int TKeySetWriter::WriteKey(const TKey& key)
{
    WireProtocolWriter_.WriteUnversionedRow(key);
    return Index_++;
}

int TKeySetWriter::WriteValueRange(TRange<TUnversionedValue> key)
{
    WireProtocolWriter_.WriteUnversionedValueRange(key);
    return Index_++;
}

TSharedRef TKeySetWriter::Finish()
{
    auto* codec = NCompression::GetCodec(NCompression::ECodec::Lz4);
    return codec->Compress(WireProtocolWriter_.Finish());
}

////////////////////////////////////////////////////////////////////////////////

TKeySetReader::TKeySetReader(const TSharedRef& compressedData)
    : WireProtocolReader_(NCompression::GetCodec(NCompression::ECodec::Lz4)->Decompress(compressedData))
{
    while (!WireProtocolReader_.IsFinished()) {
        Keys_.emplace_back(WireProtocolReader_.ReadUnversionedRow(false));
    }
}

TRange<TKey> TKeySetReader::GetKeys() const
{
    return MakeRange(Keys_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
