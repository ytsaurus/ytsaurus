#include "transaction.h"

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

} // namespace NYT::NApi
