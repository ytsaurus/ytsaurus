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
    std::optional<NTransactionClient::TTimestamp> timestamp)
{
    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        FromRecordKeys<TRecordKey>(keys, GetRowBuffer()),
        columnFilter,
        timestamp);
    return rowsetFuture.Apply(BIND([] (const NApi::IUnversionedRowsetPtr& rowset) {
        return NTableClient::ToOptionalRecords<typename TRecordKey::TRecordDescriptor::TRecord>(rowset);
    }));
}

template <class TRecord>
TFuture<std::vector<TRecord>> ISequoiaClient::SelectRows(
    const std::vector<TString>& whereConjuncts,
    std::optional<i64> limit,
    std::optional<NTransactionClient::TTimestamp> timestamp)
{
    auto resultFuture = SelectRows(TRecord::Table, whereConjuncts, limit, timestamp);
    return resultFuture.Apply(BIND([] (const NApi::TSelectRowsResult& result) {
        return NTableClient::ToRecords<TRecord>(result.Rowset);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
