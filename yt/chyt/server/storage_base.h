#pragma once

#include <Storages/IStorage.h>
#include <util/generic/fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TYtStorageBase
    : public DB::IStorage
{
public:
    explicit TYtStorageBase(DB::StorageID storageId);

protected:
    struct TRealAndVirtualColumnNames
    {
        std::vector<std::string> RealColumnNames;
        std::vector<std::string> VirtualColumnNames;
    };

    TRealAndVirtualColumnNames DecoupleColumns(
        const DB::Names& columnNames,
        const DB::StorageMetadataPtr& metadataSnapshot) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
