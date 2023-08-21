#pragma once

#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TKeySetWriter
    : public TRefCounted
{
public:
    int WriteKey(TLegacyKey key);
    int WriteValueRange(TUnversionedValueRange key);

    TSharedRef Finish();

private:
    const std::unique_ptr<IWireProtocolWriter> WireProtocolWriter_ = CreateWireProtocolWriter();

    int Index_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TKeySetWriter)

////////////////////////////////////////////////////////////////////////////////

class TKeySetReader
{
public:
    TKeySetReader(const TSharedRef& compressedData);

    TRange<TLegacyKey> GetKeys() const;

private:
    std::unique_ptr<IWireProtocolReader> WireProtocolReader_;

    std::vector<TLegacyKey> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
