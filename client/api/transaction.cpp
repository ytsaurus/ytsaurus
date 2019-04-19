#include "transaction.h"

#include <yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;

/////////////////////////////////////////////////////////////////////////////

void ITransaction::WriteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TUnversionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::Write, row.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), rows.GetHolder()),
        options);
}

void ITransaction::WriteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TVersionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::VersionedWrite, row.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), rows.GetHolder()),
        options);
}

void ITransaction::DeleteRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TKey> keys,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());
    for (auto key : keys) {
        modifications.push_back({ERowModificationType::Delete, key.ToTypeErasedRow(), TLockMask()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), keys.GetHolder()),
        options);
}

////////////////////////////////////////////////////////////////////////////////

void ITransaction::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TKey> keys,
    TLockMask lockMask)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());

    for (auto key : keys) {
        TRowModification modification;
        modification.Type = ERowModificationType::ReadLockWrite;
        modification.Row = key.ToTypeErasedRow();
        modification.Locks = lockMask;
        modifications.push_back(modification);
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), std::move(keys)),
        TModifyRowsOptions());
}

void ITransaction::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TKey> keys,
    ELockType lockType)
{
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, lockType);
    LockRows(path, nameTable, keys, lockMask);
}

void ITransaction::LockRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TKey> keys,
    const std::vector<TString>& locks,
    ELockType lockType)
{
    const auto& tableMountCache = GetClient()->GetTableMountCache();
    auto tableInfo = NConcurrency::WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    auto lockMask = GetLockMask(
        tableInfo->Schemas[NTabletClient::ETableSchemaKind::Write],
        GetAtomicity() == NTransactionClient::EAtomicity::Full,
        locks,
        lockType);

    LockRows(path, nameTable, keys, lockMask);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
