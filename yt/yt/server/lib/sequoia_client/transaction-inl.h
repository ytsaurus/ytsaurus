#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

#include <yt/yt/client/api/rowset.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> ISequoiaTransaction::LookupRows(
    const std::vector<TRecordKey>& keys,
    NTransactionClient::TTimestamp timestamp,
    const NTableClient::TColumnFilter& columnFilter)
{
    std::vector<NTableClient::TLegacyKey> rawKeys;
    rawKeys.reserve(keys.size());
    for (const auto& key : keys) {
        rawKeys.push_back(key.ToKey(GetRowBuffer()));
    }

    auto rowsetFuture = LookupRows(
        TRecordKey::Table,
        MakeSharedRange(std::move(rawKeys), GetRowBuffer()),
        timestamp,
        columnFilter);
    return rowsetFuture.Apply(BIND([=, keyCount = std::ssize(keys)] (const NApi::IUnversionedRowsetPtr& rowset) {
        auto rows = rowset->GetRows();
        YT_VERIFY(std::ssize(rows) == keyCount);

        typename TRecordKey::TRecordDescriptor::TIdMapping idMapping(rowset->GetNameTable());
        std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>> optionalRecords;
        optionalRecords.reserve(rows.size());
        for (auto row : rows) {
            if (row) {
                optionalRecords.emplace_back(TRecordKey::TRecordDescriptor::TRecord::FromUnversionedRow(row, idMapping));
            } else {
                optionalRecords.push_back(std::nullopt);
            }
        }

        return optionalRecords;
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
void ISequoiaTransaction::WriteRow(const TRecord& record)
{
    WriteRow(
        TRecord::Table,
        record.ToUnversionedRow(GetRowBuffer()));
}

template <class TRecordKey>
void ISequoiaTransaction::DeleteRow(const TRecordKey& key)
{
    DeleteRow(
        TRecordKey::Table,
        key.ToKey(GetRowBuffer()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
