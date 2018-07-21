#include "table_mount_cache.h"

#include <yt/client/object_client/helpers.h>

namespace NYT {
namespace NTabletClient {

using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool TTabletInfo::IsInMemory() const
{
    if (!InMemoryMode) {
        return false;
    }

    switch (*InMemoryMode) {
        case EInMemoryMode::None:
            return false;

        case EInMemoryMode::Compressed:
            return true;

        case EInMemoryMode::Uncompressed:
            return true;

        default:
            Y_UNREACHABLE();
    }
}

bool TTableMountInfo::IsSorted() const
{
    return Schemas[ETableSchemaKind::Primary].IsSorted();
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
    if (tabletIndex < 0 || tabletIndex >= Tablets.size()) {
        THROW_ERROR_EXCEPTION("Invalid tablet index: expected in range [0,%v], got %v",
            Tablets.size() - 1,
            tabletIndex);
    }
    return Tablets[tabletIndex];
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(const TRange<TUnversionedValue>& row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.Size() >= keyColumnCount);
    ValidateDynamic();
    auto it = std::upper_bound(
        Tablets.begin(),
        Tablets.end(),
        row,
        [&] (const TRange<TUnversionedValue>& key, const TTabletInfoPtr& rhs) {
            return CompareRows(
                key.Begin(),
                key.Begin() + keyColumnCount,
                rhs->PivotKey.Begin(),
                rhs->PivotKey.End()) < 0;
        });
    YCHECK(it != Tablets.begin());
    return *(--it);
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TUnversionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.GetCount() >= keyColumnCount);
    return GetTabletForRow(MakeRange(row.Begin(), row.Begin() + keyColumnCount));
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TVersionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.GetKeyCount() == keyColumnCount);
    return GetTabletForRow(MakeRange(row.BeginKeys(), row.EndKeys()));
}

TTabletInfoPtr TTableMountInfo::GetRandomMountedTablet() const
{
    ValidateDynamic();

    if (MountedTablets.empty()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::TabletNotMounted,
            "Table %v has no mounted tablets",
            Path);
    }

    size_t index = RandomNumber(MountedTablets.size());
    return MountedTablets[index];
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

} // namespace NTabletClient
} // namespace NYT

