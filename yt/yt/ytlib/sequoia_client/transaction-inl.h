#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/record_helpers.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> ISequoiaTransaction::LookupRows(
    const std::vector<TRecordKey>& keys,
    const NTableClient::TColumnFilter& columnFilter)
{
    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        FromRecordKeys<TRecordKey>(keys, GetRowBuffer()),
        columnFilter);
    return rowsetFuture.Apply(BIND([] (const NApi::TUnversionedLookupRowsResult& result) {
        return NTableClient::ToOptionalRecords<typename TRecordKey::TRecordDescriptor::TRecord>(result.Rowset);
    }));
}

template <class TRecord>
TFuture<std::vector<TRecord>> ISequoiaTransaction::SelectRows(
    const TSelectRowsQuery& query)
{
    auto resultFuture = SelectRows(TRecord::Table, query);
    return resultFuture.Apply(BIND([] (const NApi::TSelectRowsResult& result) {
        return NTableClient::ToRecords<TRecord>(result.Rowset);
    }));
}

template <class TRecord>
void ISequoiaTransaction::DatalessLockRow(
    NObjectClient::TCellTag masterCellTag,
    const TRecord& record,
    NTableClient::ELockType lockType)
{
    DatalessLockRow(
        masterCellTag,
        TRecord::Table,
        record.ToKey(GetRowBuffer()),
        lockType);
}

template <class TRecord>
void ISequoiaTransaction::LockRow(
    const TRecord& record,
    NTableClient::ELockType lockType)
{
    LockRow(
        TRecord::Table,
        record.ToKey(GetRowBuffer()),
        lockType);
}

template <class TRecord>
void ISequoiaTransaction::WriteRow(
    const TRecord& record,
    NTableClient::ELockType lockType,
    NTableClient::EValueFlags flags)
{
    WriteRow(
        TRecord::Table,
        NTableClient::FromRecord(
            record,
            GetRowBuffer(),
            TRecord::TRecordDescriptor::Get()->GetIdMapping(),
            flags),
        lockType);
}

template <class TRecord>
void ISequoiaTransaction::WriteRow(
    NObjectClient::TCellTag masterCellTag,
    const TRecord& record,
    NTableClient::ELockType lockType,
    NTableClient::EValueFlags flags)
{
    TSequoiaTablePathDescriptor descriptor{
        .Table = TRecord::Table,
        .MasterCellTag = masterCellTag,
    };
    WriteRow(
        descriptor,
        NTableClient::FromRecord(
            record,
            GetRowBuffer(),
            TRecord::TRecordDescriptor::Get()->GetIdMapping(),
            flags),
        lockType);
}

template <class TRecordKey>
void ISequoiaTransaction::DeleteRow(const TRecordKey& key)
{
    DeleteRow(
        TRecordKey::Table,
        NTableClient::FromRecordKey(key, GetRowBuffer()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
