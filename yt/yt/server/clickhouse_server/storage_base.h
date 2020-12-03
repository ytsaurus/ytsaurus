#pragma once

#include <Storages/IStorage.h>
#include <util/generic/fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TYtStorageBase
    : public DB::IStorage
{
public:
    TYtStorageBase(DB::StorageID storageId);

    virtual DB::NamesAndTypesList getVirtuals() const override;

protected:
    struct TRealAndVirtualColumnNames
    {
        std::vector<TString> RealColumnNames;
        std::vector<TString> VirtualColumnNames;
    };

    TRealAndVirtualColumnNames DecoupleColumns(
        const DB::Names& columnNames,
        const DB::StorageMetadataPtr& metadataSnapshot) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
