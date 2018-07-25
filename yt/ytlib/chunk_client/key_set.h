#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/table_client/wire_protocol.h>

namespace NYT {
namespace NChunkClient {

// ToDo(psushin): move to NTableClient.

////////////////////////////////////////////////////////////////////////////////

class TKeySetWriter
    : public TRefCounted
{
public:
    int WriteKey(const NTableClient::TKey& key);
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

    TRange<NTableClient::TKey> GetKeys() const;

private:
    NTableClient::TWireProtocolReader WireProtocolReader_;
    std::vector<NTableClient::TKey> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
