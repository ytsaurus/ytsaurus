#include "transaction.h"

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/ytlib/table_client/helpers.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void ITransaction::WriteRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TUnversionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::Write, row.ToTypeErasedRow()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), rows.GetHolder()),
        options);
}

void ITransaction::WriteRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TVersionedRow> rows,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(rows.Size());

    for (auto row : rows) {
        modifications.push_back({ERowModificationType::VersionedWrite, row.ToTypeErasedRow()});
    }

    ModifyRows(
        path,
        std::move(nameTable),
        MakeSharedRange(std::move(modifications), rows.GetHolder()),
        options);
}

void ITransaction::DeleteRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TKey> keys,
    const TModifyRowsOptions& options)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());
    for (auto key : keys) {
        modifications.push_back({ERowModificationType::Delete, key.ToTypeErasedRow()});
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
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TKey> keys,
    ui32 lockMask)
{
    std::vector<TRowModification> modifications;
    modifications.reserve(keys.Size());

    for (auto key : keys) {
        TRowModification modification;
        modification.Type = ERowModificationType::ReadLockWrite;
        modification.Row = key.ToTypeErasedRow();
        modification.ReadLocks = lockMask;
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
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TKey> keys)
{
    LockRows(path, std::move(nameTable), std::move(keys), NTableClient::PrimaryLockMask);
}

void ITransaction::LockRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TKey> keys,
    const std::vector<TString>& locks)
{
    const auto& tableMountCache = GetClient()->GetTableMountCache();
    auto tableInfo = NConcurrency::WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    ui32 lockMask = GetLockMask(
        tableInfo->Schemas[NTabletClient::ETableSchemaKind::Write],
        GetAtomicity() == NTransactionClient::EAtomicity::Full,
        locks);

    LockRows(path, nameTable, keys, lockMask);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
