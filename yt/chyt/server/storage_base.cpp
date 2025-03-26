#include "storage_base.h"

#include "private.h"
#include "virtual_column.h"

#include <util/generic/string.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TYtStorageBase::TYtStorageBase(DB::StorageID storageId)
    : DB::IStorage(std::move(storageId))
{
    setVirtuals(VirtualColumns);
}

TYtStorageBase::TRealAndVirtualColumnNames TYtStorageBase::DecoupleColumns(
    const DB::Names& columnNames,
    const DB::StorageMetadataPtr& metadataSnapshot) const
{
    TRealAndVirtualColumnNames result;
    result.RealColumnNames.reserve(columnNames.size());

    for (const auto& column : columnNames) {
        if (isVirtualColumn(column, metadataSnapshot)) {
            result.VirtualColumnNames.push_back(column);
        } else {
            result.RealColumnNames.push_back(column);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
