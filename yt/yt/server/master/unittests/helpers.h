#pragma once

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapMock
    : public NCellMaster::TBootstrap
{
public:
    TBootstrapMock();

    void SetupMasterSmartpointers();
    void ResetMasterSmartpointers();
};

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateChunkId();
TGuid GenerateChunkListId();
TGuid GenerateTabletCellId();
TGuid GenerateTabletCellBundleId();
TGuid GenerateClusterNodeId();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
