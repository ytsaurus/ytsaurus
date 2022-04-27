#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void ISequoiaTransaction::DatalessLockRow(
    NObjectClient::TCellTag masterCellTag,
    const TRow& row,
    NTableClient::ELockType lockType)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    DatalessLockRow(
        masterCellTag,
        tableDescriptor->GetType(),
        tableDescriptor->ToKey(row, GetRowBuffer()),
        lockType);
}

template <class TRow>
void ISequoiaTransaction::LockRow(
    const TRow& row,
    NTableClient::ELockType lockType)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    LockRow(
        tableDescriptor->GetType(),
        tableDescriptor->ToKey(row, GetRowBuffer()),
        lockType);
}

template <class TRow>
void ISequoiaTransaction::WriteRow(const TRow& row)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    WriteRow(
        tableDescriptor->GetType(),
        tableDescriptor->ToUnversionedRow(row, GetRowBuffer()));
}

template <class TRow>
void ISequoiaTransaction::DeleteRow(const TRow& row)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    DeleteRow(
        tableDescriptor->GetType(),
        tableDescriptor->ToKey(row, GetRowBuffer()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
