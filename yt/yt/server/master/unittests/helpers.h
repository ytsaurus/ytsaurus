#pragma once

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/core/test_framework/framework.h>

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

class TBootstrapTestBase
    : public ::testing::Test
{
public:
    void SetUp() override;
    void TearDown() override;

private:
    const TIntrusivePtr<TBootstrapMock> Bootstrap_ = New<TBootstrapMock>();
};

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateChunkId();
TGuid GenerateChunkListId();
TGuid GenerateTabletCellId();
TGuid GenerateTabletCellBundleId();
TGuid GenerateClusterNodeId();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
