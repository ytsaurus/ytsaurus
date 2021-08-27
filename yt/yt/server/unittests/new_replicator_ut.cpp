#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/chunk_server/new_replicator/data_center.h>
#include <yt/yt/server/master/chunk_server/new_replicator/medium.h>
#include <yt/yt/server/master/chunk_server/new_replicator/replicator_state.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NChunkServer::NReplicator {
namespace {

using namespace NConcurrency;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

struct TReplicatorStateProxy
    : public IReplicatorStateProxy
{
    const IInvokerPtr& GetChunkInvoker(EChunkThreadQueue /*queue*/) const override
    {
        return Queue->GetInvoker();
    }

    const TDynamicClusterConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig;
    }

    std::vector<NChunkServer::TMedium*> GetMedia() const override
    {
        return Media;
    }

    std::vector<NNodeTrackerServer::TDataCenter*> GetDataCenters() const override
    {
        return DataCenters;
    }

    bool CheckThreadAffinity() const override
    {
        return false;
    }

    TActionQueuePtr Queue = New<TActionQueue>();

    TDynamicClusterConfigPtr DynamicConfig = New<TDynamicClusterConfig>();

    std::vector<NChunkServer::TMedium*> Media;
    std::vector<NNodeTrackerServer::TDataCenter*> DataCenters;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatorTest
    : public ::testing::Test
{
protected:
    TReplicatorStateProxy* Proxy_;

    IReplicatorStatePtr ReplicatorState_;

    void SetUp() override
    {
        auto proxy = std::make_unique<TReplicatorStateProxy>();
        Proxy_ = proxy.get();

        ReplicatorState_ = CreateReplicatorState(std::move(proxy));
    }

    static std::unique_ptr<NChunkServer::TMedium> CreateMedium(
        TMediumId mediumId,
        TMediumIndex mediumIndex,
        const TString& name,
        const TMediumConfigPtr& config = New<TMediumConfig>())
    {
        auto medium = TPoolAllocator::New<NChunkServer::TMedium>(mediumId);
        medium->SetIndex(mediumIndex);
        medium->SetName(name);
        medium->Config() = config;

        return medium;
    }

    static std::unique_ptr<NNodeTrackerServer::TDataCenter> CreateDataCenter(
        TDataCenterId dataCenterId,
        const TString& name)
    {
        auto dataCenter = TPoolAllocator::New<NNodeTrackerServer::TDataCenter>(dataCenterId);
        dataCenter->SetName(name);

        return dataCenter;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TReplicatorTest, TestDynamicConfigReplication)
{
    Proxy_->DynamicConfig->ChunkManager->MaxChunksPerFetch = 123;

    ReplicatorState_->Load();

    EXPECT_EQ(ReplicatorState_->GetDynamicConfig()->ChunkManager->MaxChunksPerFetch, 123);
}

TEST_F(TReplicatorTest, TestUpdateDynamicConfig)
{
    auto newConfig = New<TDynamicClusterConfig>();
    newConfig->ChunkManager->MaxChunksPerFetch = 234;
    ReplicatorState_->UpdateDynamicConfig(newConfig);
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(ReplicatorState_->GetDynamicConfig()->ChunkManager->MaxChunksPerFetch, 234);
}

TEST_F(TReplicatorTest, TestMediumReplication)
{
    auto mediumConfig = New<TMediumConfig>();
    mediumConfig->MaxReplicasPerRack = 5;

    auto medium = CreateMedium(/*mediumId*/ TGuid::Create(), /*mediumIndex*/ 42, "ssd", mediumConfig);
    Proxy_->Media.push_back(medium.get());

    ReplicatorState_->Load();

    auto* dualMedium = ReplicatorState_->FindMedium(medium->GetId());
    EXPECT_EQ(dualMedium->GetId(), medium->GetId());
    EXPECT_EQ(dualMedium->GetIndex(), 42);
    EXPECT_EQ(dualMedium->Name(), "ssd");
    EXPECT_EQ(dualMedium->Config()->MaxReplicasPerRack, 5);

    EXPECT_EQ(ReplicatorState_->FindMediumByIndex(42), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByIndex(43), nullptr);

    EXPECT_EQ(ReplicatorState_->FindMediumByName("ssd"), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByName("hdd"), nullptr);

    EXPECT_EQ(ReplicatorState_->FindMedium(medium->GetId()), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMedium(TGuid::Create()), nullptr);

    const auto& mediaMap = ReplicatorState_->Media();
    EXPECT_EQ(std::ssize(mediaMap), 1);
    EXPECT_EQ(GetOrCrash(mediaMap, medium->GetId()).get(), dualMedium);
}

TEST_F(TReplicatorTest, TestCreateMedium)
{
    auto mediumConfig = New<TMediumConfig>();
    mediumConfig->MaxReplicasPerRack = 5;

    auto medium = CreateMedium(/*mediumId*/ TGuid::Create(), /*mediumIndex*/ 42, "ssd", mediumConfig);
    Proxy_->Media.push_back(medium.get());

    ReplicatorState_->CreateMedium(medium.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualMedium = ReplicatorState_->FindMedium(medium->GetId());
    EXPECT_EQ(dualMedium->GetId(), medium->GetId());
    EXPECT_EQ(dualMedium->GetIndex(), 42);
    EXPECT_EQ(dualMedium->Name(), "ssd");
    EXPECT_EQ(dualMedium->Config()->MaxReplicasPerRack, 5);

    EXPECT_EQ(ReplicatorState_->FindMediumByIndex(42), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByIndex(43), nullptr);

    EXPECT_EQ(ReplicatorState_->FindMediumByName("ssd"), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByName("hdd"), nullptr);

    EXPECT_EQ(ReplicatorState_->FindMedium(medium->GetId()), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMedium(TGuid::Create()), nullptr);

    const auto& mediaMap = ReplicatorState_->Media();
    EXPECT_EQ(std::ssize(mediaMap), 1);
    EXPECT_EQ(GetOrCrash(mediaMap, medium->GetId()).get(), dualMedium);
}

TEST_F(TReplicatorTest, TestRenameMedium)
{
    auto medium = CreateMedium(/*mediumId*/ TGuid::Create(), /*mediumIndex*/ 42, "ssd");
    Proxy_->Media.push_back(medium.get());

    ReplicatorState_->CreateMedium(medium.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualMedium = ReplicatorState_->FindMedium(medium->GetId());
    EXPECT_EQ(dualMedium->Name(), "ssd");

    EXPECT_EQ(ReplicatorState_->FindMediumByName("ssd"), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByName("hdd"), nullptr);

    ReplicatorState_->RenameMedium(medium->GetId(), "hdd");
    ReplicatorState_->SyncWithUpstream();
    EXPECT_EQ(dualMedium->Name(), "hdd");

    EXPECT_EQ(ReplicatorState_->FindMediumByName("hdd"), dualMedium);
    EXPECT_EQ(ReplicatorState_->FindMediumByName("ssd"), nullptr);
}

TEST_F(TReplicatorTest, TestUpdateMediumConfig)
{
    auto mediumConfig = New<TMediumConfig>();
    mediumConfig->MaxReplicasPerRack = 5;

    auto medium = CreateMedium(/*mediumId*/ TGuid::Create(), /*mediumIndex*/ 42, "ssd", mediumConfig);
    Proxy_->Media.push_back(medium.get());

    ReplicatorState_->CreateMedium(medium.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualMedium = ReplicatorState_->FindMedium(medium->GetId());
    EXPECT_EQ(dualMedium->Config()->MaxReplicasPerRack, 5);

    mediumConfig->MaxReplicasPerRack = 10;

    ReplicatorState_->UpdateMediumConfig(medium->GetId(), mediumConfig);
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(dualMedium->Config()->MaxReplicasPerRack, 10);
}

TEST_F(TReplicatorTest, TestDataCenterReplication)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "N");
    Proxy_->DataCenters.push_back(dataCenter.get());

    ReplicatorState_->Load();

    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    EXPECT_EQ(dualDataCenter->GetId(), dataCenter->GetId());
    EXPECT_EQ(dualDataCenter->Name(), "N");

    EXPECT_EQ(ReplicatorState_->FindDataCenter(dataCenter->GetId()), dualDataCenter);
    EXPECT_EQ(ReplicatorState_->FindDataCenter(TGuid::Create()), nullptr);

    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("N"), dualDataCenter);
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("M"), nullptr);

    const auto& dataCenterMap = ReplicatorState_->DataCenters();
    EXPECT_EQ(std::ssize(dataCenterMap), 1);
    EXPECT_EQ(GetOrCrash(dataCenterMap, dataCenter->GetId()).get(), dualDataCenter);
}

TEST_F(TReplicatorTest, TestCreateDestroyDataCenter)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "N");
    Proxy_->DataCenters.push_back(dataCenter.get());

    ReplicatorState_->CreateDataCenter(dataCenter.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    EXPECT_EQ(dualDataCenter->GetId(), dataCenter->GetId());
    EXPECT_EQ(dualDataCenter->Name(), "N");

    EXPECT_EQ(ReplicatorState_->FindDataCenter(dataCenter->GetId()), dualDataCenter);
    EXPECT_EQ(ReplicatorState_->FindDataCenter(TGuid::Create()), nullptr);

    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("N"), dualDataCenter);
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("M"), nullptr);

    const auto& dataCenterMap = ReplicatorState_->DataCenters();
    EXPECT_EQ(std::ssize(dataCenterMap), 1);
    EXPECT_EQ(GetOrCrash(dataCenterMap, dataCenter->GetId()).get(), dualDataCenter);

    ReplicatorState_->DestroyDataCenter(dataCenter->GetId());
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(ReplicatorState_->FindDataCenter(dataCenter->GetId()), nullptr);
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("N"), nullptr);
    EXPECT_TRUE(dataCenterMap.empty());
}

TEST_F(TReplicatorTest, TestRenameDataCenter)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "N");
    Proxy_->DataCenters.push_back(dataCenter.get());

    ReplicatorState_->CreateDataCenter(dataCenter.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());

    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("N"), dualDataCenter);
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("M"), nullptr);

    ReplicatorState_->RenameDataCenter(dataCenter->GetId(), "M");
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(dualDataCenter->Name(), "M");
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("N"), nullptr);
    EXPECT_EQ(ReplicatorState_->FindDataCenterByName("M"), dualDataCenter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // NYT::NChunkServer::NReplicator
