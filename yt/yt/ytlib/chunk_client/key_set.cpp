#include "key_set.h"

#include <yt/yt/core/compression/codec.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

int TKeySetWriter::WriteKey(const TLegacyKey& key)
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

TRange<TLegacyKey> TKeySetReader::GetKeys() const
{
    return MakeRange(Keys_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
