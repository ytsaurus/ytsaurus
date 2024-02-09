#ifndef CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include client.h"
// For the sake of sane code completion.
#include "client.h"
#endif

#include <yt/yt/client/table_client/record_helpers.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> ISequoiaClient::LookupRows(
    const std::vector<TRecordKey>& keys,
    const NTableClient::TColumnFilter& columnFilter,
    NTransactionClient::TTimestamp timestamp)
{
    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        NTableClient::FromRecordKeys<TRecordKey>(keys),
        columnFilter,
        timestamp);
    return rowsetFuture.Apply(BIND([] (const NApi::TUnversionedLookupRowsResult& result) {
        return NTableClient::ToOptionalRecords<typename TRecordKey::TRecordDescriptor::TRecord>(result.Rowset);
    }));
}

template <class TRecord>
TFuture<std::vector<TRecord>> ISequoiaClient::SelectRows(
    const TSelectRowsRequest& request,
    NTransactionClient::TTimestamp timestamp)
{
    auto resultFuture = SelectRows(TRecord::Table, request, timestamp);
    return resultFuture.Apply(BIND([] (const NApi::TSelectRowsResult& result) {
        return NTableClient::ToRecords<TRecord>(result.Rowset);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
