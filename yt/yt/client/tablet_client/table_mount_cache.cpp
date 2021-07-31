#include "table_mount_cache.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/key_bound.h>

namespace NYT::NTabletClient {

using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TKeyBound TTabletInfo::GetLowerKeyBound() const
{
    return TKeyBound::FromRow() >= PivotKey;
}

////////////////////////////////////////////////////////////////////////////////

bool TTableMountInfo::IsSorted() const
{
    return Schemas[ETableSchemaKind::Primary]->IsSorted();
}

bool TTableMountInfo::IsOrdered() const
{
    return !IsSorted();
}

bool TTableMountInfo::IsReplicated() const
{
    return TypeFromId(TableId) == EObjectType::ReplicatedTable;
}

TTabletInfoPtr TTableMountInfo::GetTabletByIndexOrThrow(int tabletIndex) const
{
    if (tabletIndex < 0 || tabletIndex >= std::ssize(Tablets)) {
        THROW_ERROR_EXCEPTION("Invalid tablet index: expected in range [0,%v], got %v",
            Tablets.size() - 1,
            tabletIndex);
    }
    return Tablets[tabletIndex];
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TRange<TUnversionedValue> row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    YT_VERIFY(std::ssize(row) >= keyColumnCount);
    ValidateDynamic();
    auto it = std::upper_bound(
        Tablets.begin(),
        Tablets.end(),
        row,
        [&] (TRange<TUnversionedValue> key, const TTabletInfoPtr& rhs) {
            return CompareRows(
                key.Begin(),
                key.Begin() + keyColumnCount,
                rhs->PivotKey.Begin(),
                rhs->PivotKey.End()) < 0;
        });
    YT_VERIFY(it != Tablets.begin());
    return *(--it);
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TUnversionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    YT_VERIFY(static_cast<int>(row.GetCount()) >= keyColumnCount);
    return GetTabletForRow(MakeRange(row.Begin(), row.Begin() + keyColumnCount));
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TVersionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    YT_VERIFY(row.GetKeyCount() == keyColumnCount);
    return GetTabletForRow(MakeRange(row.BeginKeys(), row.EndKeys()));
}

int TTableMountInfo::GetRandomMountedTabletIndex() const
{
    ValidateDynamic();

    if (MountedTablets.empty()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::TabletNotMounted,
            "Table %v has no mounted tablets",
            Path);
    }

    return RandomNumber(MountedTablets.size());
}

TTabletInfoPtr TTableMountInfo::GetRandomMountedTablet() const
{
    return MountedTablets[GetRandomMountedTabletIndex()];
}

void TTableMountInfo::ValidateDynamic() const
{
    if (!Dynamic) {
        THROW_ERROR_EXCEPTION("Table %v is not dynamic", Path);
    }
}

void TTableMountInfo::ValidateSorted() const
{
    if (!IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted", Path);
    }
}

void TTableMountInfo::ValidateOrdered() const
{
    if (!IsOrdered()) {
        THROW_ERROR_EXCEPTION("Table %v is not ordered", Path);
    }
}

void TTableMountInfo::ValidateNotReplicated() const
{
    if (IsReplicated()) {
        THROW_ERROR_EXCEPTION("Table %v is replicated", Path);
    }
}

void TTableMountInfo::ValidateReplicated() const
{
    if (!IsReplicated()) {
        THROW_ERROR_EXCEPTION("Table %v is not replicated", Path);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

