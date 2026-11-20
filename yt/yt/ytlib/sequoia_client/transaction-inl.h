#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

#include "record_helpers.h"

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/record_helpers.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> ISequoiaTransaction::LookupRows(
    const std::vector<TRecordKey>& recordKeys,
    const NTableClient::TColumnFilter& columnFilter)
{
    auto keys = FromRecordKeys<TRecordKey>(recordKeys, GetRowBuffer().Get());
    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        keys,
        columnFilter);
    return rowsetFuture.Apply(BIND([] (const NApi::TUnversionedLookupRowsResult& result) {
        return NTableClient::ToOptionalRecords<typename TRecordKey::TRecordDescriptor::TRecord>(result.Rowset);
    }));
}

template <class TRecordKey>
TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecordPartial>>>
ISequoiaTransaction::LookupRowsPartial(
    const std::vector<TRecordKey>& recordKeys,
    const NTableClient::TColumnFilter& columnFilter)
{
    auto keys = FromRecordKeys<TRecordKey>(recordKeys, GetRowBuffer().Get());
    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        keys,
        columnFilter);
    return rowsetFuture.Apply(BIND([] (const NApi::TUnversionedLookupRowsResult& result) {
        return NTableClient::ToOptionalRecords<typename TRecordKey::TRecordDescriptor::TRecordPartial>(result.Rowset);
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
    TStrongOrderingTags strongOrderingTags;
    if (GetFeatures().UseFineGrainedOrderingTags.value_or(false)) {
        strongOrderingTags = GetStrongOrderingTags(record);
    }

    auto key = record.ToKey(GetRowBuffer().Get());
    DatalessLockRow(
        masterCellTag,
        TRecord::Table,
        key,
        std::move(strongOrderingTags),
        lockType);
}

template <class TRecord>
void ISequoiaTransaction::LockRow(
    const TRecord& record,
    NTableClient::ELockType lockType)
{
    auto key = record.ToKey(GetRowBuffer().Get());

    TStrongOrderingTags strongOrderingTags;
    if (GetFeatures().UseFineGrainedOrderingTags.value_or(false)) {
        strongOrderingTags = GetStrongOrderingTags(record);
    }

    LockRow(
        TRecord::Table,
        key,
        std::move(strongOrderingTags),
        lockType);
}

template <class TRecord>
void ISequoiaTransaction::WriteRow(
    const TRecord& record,
    NTableClient::ELockType lockType,
    NTableClient::EValueFlags flags)
{
    auto row = NTableClient::FromRecord(
        record,
        GetRowBuffer().Get(),
        TRecord::TRecordDescriptor::Get()->GetPartialIdMapping(),
        flags);

    TStrongOrderingTags strongOrderingTags;
    if (GetFeatures().UseFineGrainedOrderingTags.value_or(false)) {
        strongOrderingTags = GetStrongOrderingTags(record);
    }

    WriteRow(
        TRecord::Table,
        row,
        std::move(strongOrderingTags),
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
    auto row = NTableClient::FromRecord(
        record,
        GetRowBuffer().Get(),
        TRecord::TRecordDescriptor::Get()->GetPartialIdMapping(),
        flags);

    TStrongOrderingTags strongOrderingTags;
    if (GetFeatures().UseFineGrainedOrderingTags.value_or(false)) {
        strongOrderingTags = GetStrongOrderingTags(record);
    }

    WriteRow(
        descriptor,
        row,
        std::move(strongOrderingTags),
        lockType);
}

template <class TRecordKey>
void ISequoiaTransaction::DeleteRow(const TRecordKey& recordKey)
{
    auto key = NTableClient::FromRecordKey(recordKey, GetRowBuffer().Get());

    TStrongOrderingTags strongOrderingTags;
    if (GetFeatures().UseFineGrainedOrderingTags.value_or(false)) {
        strongOrderingTags = GetStrongOrderingTags(recordKey);
    }

    DeleteRow(TRecordKey::Table, key, std::move(strongOrderingTags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
