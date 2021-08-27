#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/chunk_server/new_replicator/data_center.h>
#include <yt/yt/server/master/chunk_server/new_replicator/medium.h>
#include <yt/yt/server/master/chunk_server/new_replicator/rack.h>
#include <yt/yt/server/master/chunk_server/new_replicator/replicator_state.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

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

    std::vector<NNodeTrackerServer::TRack*> GetRacks() const override
    {
        return Racks;
    }

    bool CheckThreadAffinity() const override
    {
        return false;
    }

    TActionQueuePtr Queue = New<TActionQueue>();

    TDynamicClusterConfigPtr DynamicConfig = New<TDynamicClusterConfig>();

    std::vector<NChunkServer::TMedium*> Media;
    std::vector<NNodeTrackerServer::TDataCenter*> DataCenters;
    std::vector<NNodeTrackerServer::TRack*> Racks;
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

    static std::unique_ptr<NNodeTrackerServer::TRack> CreateRack(
        TRackId rackId,
        TRackIndex rackIndex,
        const TString& name,
        NNodeTrackerServer::TDataCenter* dataCenter)
    {
        auto rack = TPoolAllocator::New<NNodeTrackerServer::TRack>(rackId);
        rack->SetIndex(rackIndex);
        rack->SetName(name);
        rack->SetDataCenter(dataCenter);

        return rack;
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

TEST_F(TReplicatorTest, TestRackReplication)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "d1");
    Proxy_->DataCenters.push_back(dataCenter.get());

    auto rack1 = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 42, "r1", /*dataCenter*/ nullptr);
    auto rack2 = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 57, "r2", /*dataCenter*/ dataCenter.get());
    Proxy_->Racks = {rack1.get(), rack2.get()};

    ReplicatorState_->Load();

    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    auto* dualRack1 = ReplicatorState_->FindRack(rack1->GetId());
    auto* dualRack2 = ReplicatorState_->FindRack(rack2->GetId());
    EXPECT_EQ(dualRack1->GetId(), rack1->GetId());
    EXPECT_EQ(dualRack1->GetIndex(), 42);
    EXPECT_EQ(dualRack1->Name(), "r1");
    EXPECT_EQ(dualRack1->GetDataCenter(), nullptr);
    EXPECT_EQ(dualRack2->GetId(), rack2->GetId());
    EXPECT_EQ(dualRack2->GetIndex(), 57);
    EXPECT_EQ(dualRack2->Name(), "r2");
    EXPECT_EQ(dualRack2->GetDataCenter(), dualDataCenter);

    EXPECT_EQ(ReplicatorState_->FindRack(rack1->GetId()), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRack(rack2->GetId()), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRack(TGuid::Create()), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByName("r1"), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r2"), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r3"), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByIndex(42), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRackByIndex(57), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r3"), nullptr);

    const auto& rackMap = ReplicatorState_->Racks();
    EXPECT_EQ(std::ssize(rackMap), 2);
    EXPECT_EQ(GetOrCrash(rackMap, rack1->GetId()).get(), dualRack1);
    EXPECT_EQ(GetOrCrash(rackMap, rack2->GetId()).get(), dualRack2);
}

TEST_F(TReplicatorTest, TestCreateDestroyRacks)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "d1");
    Proxy_->DataCenters.push_back(dataCenter.get());

    ReplicatorState_->Load();

    auto rack1 = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 42, "r1", /*dataCenter*/ nullptr);
    auto rack2 = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 57, "r2", /*dataCenter*/ dataCenter.get());
    ReplicatorState_->CreateRack(rack1.get());
    ReplicatorState_->CreateRack(rack2.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    auto* dualRack1 = ReplicatorState_->FindRack(rack1->GetId());
    auto* dualRack2 = ReplicatorState_->FindRack(rack2->GetId());
    EXPECT_EQ(dualRack1->GetId(), rack1->GetId());
    EXPECT_EQ(dualRack1->GetIndex(), 42);
    EXPECT_EQ(dualRack1->Name(), "r1");
    EXPECT_EQ(dualRack1->GetDataCenter(), nullptr);
    EXPECT_EQ(dualRack2->GetId(), rack2->GetId());
    EXPECT_EQ(dualRack2->GetIndex(), 57);
    EXPECT_EQ(dualRack2->Name(), "r2");
    EXPECT_EQ(dualRack2->GetDataCenter(), dualDataCenter);

    EXPECT_EQ(ReplicatorState_->FindRack(rack1->GetId()), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRack(rack2->GetId()), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRack(TGuid::Create()), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByName("r1"), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r2"), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r3"), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByIndex(42), dualRack1);
    EXPECT_EQ(ReplicatorState_->FindRackByIndex(57), dualRack2);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r3"), nullptr);

    const auto& rackMap = ReplicatorState_->Racks();
    EXPECT_EQ(std::ssize(rackMap), 2);
    EXPECT_EQ(GetOrCrash(rackMap, rack1->GetId()).get(), dualRack1);
    EXPECT_EQ(GetOrCrash(rackMap, rack2->GetId()).get(), dualRack2);

    ReplicatorState_->DestroyRack(rack1->GetId());
    ReplicatorState_->DestroyRack(rack2->GetId());
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(ReplicatorState_->FindRack(rack1->GetId()), nullptr);
    EXPECT_EQ(ReplicatorState_->FindRack(rack2->GetId()), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByName("r1"), nullptr);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r2"), nullptr);

    EXPECT_EQ(ReplicatorState_->FindRackByIndex(42), nullptr);
    EXPECT_EQ(ReplicatorState_->FindRackByIndex(57), nullptr);

    EXPECT_TRUE(rackMap.empty());
}

TEST_F(TReplicatorTest, TestRenameRack)
{
    auto rack = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 42, "r1", /*dataCenter*/ nullptr);
    ReplicatorState_->CreateRack(rack.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualRack = ReplicatorState_->FindRack(rack->GetId());
    EXPECT_EQ(dualRack->Name(), "r1");
    EXPECT_EQ(ReplicatorState_->FindRackByName("r1"), dualRack);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r2"), nullptr);

    ReplicatorState_->RenameRack(dualRack->GetId(), "r2");
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(dualRack->Name(), "r2");
    EXPECT_EQ(ReplicatorState_->FindRackByName("r1"), nullptr);
    EXPECT_EQ(ReplicatorState_->FindRackByName("r2"), dualRack);
}

TEST_F(TReplicatorTest, TestSetRackDataCenter)
{
    auto rack = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 42, "r1", /*dataCenter*/ nullptr);
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "Narnia");
    ReplicatorState_->CreateRack(rack.get());
    ReplicatorState_->CreateDataCenter(dataCenter.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualRack = ReplicatorState_->FindRack(rack->GetId());
    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    EXPECT_EQ(dualRack->GetDataCenter(), nullptr);

    ReplicatorState_->SetRackDataCenter(rack->GetId(), dataCenter->GetId());
    ReplicatorState_->SyncWithUpstream();
    EXPECT_EQ(dualRack->GetDataCenter(), dualDataCenter);

    ReplicatorState_->SetRackDataCenter(rack->GetId(), TDataCenterId());
    ReplicatorState_->SyncWithUpstream();
    EXPECT_EQ(dualRack->GetDataCenter(), nullptr);
}

TEST_F(TReplicatorTest, TestUnbindRackOnDataCenterDestroy)
{
    auto dataCenter = CreateDataCenter(/*dataCenterId*/ TGuid::Create(), "Narnia");
    auto rack = CreateRack(/*rackId*/ TGuid::Create(), /*rackIndex*/ 42, "r1", /*dataCenter*/ dataCenter.get());
    ReplicatorState_->CreateDataCenter(dataCenter.get());
    ReplicatorState_->CreateRack(rack.get());
    ReplicatorState_->SyncWithUpstream();

    auto* dualRack = ReplicatorState_->FindRack(rack->GetId());
    auto* dualDataCenter = ReplicatorState_->FindDataCenter(dataCenter->GetId());
    EXPECT_EQ(dualRack->GetDataCenter(), dualDataCenter);

    ReplicatorState_->DestroyDataCenter(dualDataCenter->GetId());
    ReplicatorState_->SyncWithUpstream();

    EXPECT_EQ(dualRack->GetDataCenter(), nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // NYT::NChunkServer::NReplicator
