#pragma once

#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NChunkClient {

// ToDo(psushin): move to NTableClient.

////////////////////////////////////////////////////////////////////////////////

class TKeySetWriter
    : public TRefCounted
{
public:
    int WriteKey(const NTableClient::TLegacyKey& key);
    int WriteValueRange(TRange<NTableClient::TUnversionedValue> key);

    TSharedRef Finish();

private:
    NTableClient::TWireProtocolWriter WireProtocolWriter_;
    int Index_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TKeySetWriter)

////////////////////////////////////////////////////////////////////////////////

class TKeySetReader
{
public:
    TKeySetReader(const TSharedRef& compressedData);

    TRange<NTableClient::TLegacyKey> GetKeys() const;

private:
    NTableClient::TWireProtocolReader WireProtocolReader_;
    std::vector<NTableClient::TLegacyKey> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
