#include "key_set.h"

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Changing this requires non-trivial compats.
static constexpr NCompression::ECodec KeySetCompressionCodec = NCompression::ECodec::Lz4;

////////////////////////////////////////////////////////////////////////////////

int TKeySetWriter::WriteKey(TLegacyKey key)
{
    WireProtocolWriter_->WriteUnversionedRow(key);
    return Index_++;
}

int TKeySetWriter::WriteValueRange(TUnversionedValueRange key)
{
    WireProtocolWriter_->WriteUnversionedValueRange(key);
    return Index_++;
}

TSharedRef TKeySetWriter::Finish()
{
    auto* codec = NCompression::GetCodec(KeySetCompressionCodec);
    return codec->Compress(WireProtocolWriter_->Finish());
}

////////////////////////////////////////////////////////////////////////////////

TKeySetReader::TKeySetReader(const TSharedRef& compressedData)
    : WireProtocolReader_(CreateWireProtocolReader(
        NCompression::GetCodec(KeySetCompressionCodec)->Decompress(compressedData)))
{
    while (!WireProtocolReader_->IsFinished()) {
        Keys_.emplace_back(WireProtocolReader_->ReadUnversionedRow(/*captureValues*/ false));
    }
}

TRange<TLegacyKey> TKeySetReader::GetKeys() const
{
    return MakeRange(Keys_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
