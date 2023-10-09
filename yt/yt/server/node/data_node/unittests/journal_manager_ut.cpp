#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/chunk_detail.h>
#include <yt/yt/server/node/data_node/blob_reader_cache.h>
#include <yt/yt/server/node/data_node/chunk_reader_sweeper.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/chunk_meta_manager.h>
#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/journal_manager.h>

#include <yt/yt/server/lib/hydra/file_changelog.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/testing/common/env.h>

namespace NYT::NDataNode {
namespace {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NDataNode;
using namespace ::testing;

static NLogging::TLogger Logger{"JournalTest"};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFakeChunkStoreHost)

class TFakeChunkStoreHost
    : public IChunkStoreHost
{
public:
    void ScheduleMasterHeartbeat() override
    { }

    NObjectClient::TCellId GetCellId() override
    {
        return TGuid::FromString("1-2-3-4");
    }

    void SubscribePopulateAlerts(TCallback<void(std::vector<TError>*)> /*callback*/) override
    { }

    NClusterNode::TMasterEpoch GetMasterEpoch() override
    {
        return 1;
    }

    INodeMemoryTrackerPtr GetMemoryUsageTracker() override
    {
        return MemoryUsageTracker_;
    }

    void CancelLocationSessions(const TChunkLocationPtr& /*location*/) override
    { }

private:
    const INodeMemoryTrackerPtr MemoryUsageTracker_ = CreateNodeMemoryTracker(1_GBs);
};

DEFINE_REFCOUNTED_TYPE(TFakeChunkStoreHost)

////////////////////////////////////////////////////////////////////////////////

class TJournalTest
    : public ::testing::Test
{
protected:
    const IBlockCachePtr BlockCache_ = GetNullBlockCache();
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("JournalTest");

    const TDataNodeConfigPtr Config_ = New<TDataNodeConfig>();
    const TClusterNodeDynamicConfigPtr DynamicConfig_ = New<TClusterNodeDynamicConfig>();
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(DynamicConfig_);

    const INodeMemoryTrackerPtr MemoryTracker_ = CreateNodeMemoryTracker(1_GBs);
    const IChunkMetaManagerPtr ChunkMetaManager_ = CreateChunkMetaManager(
        Config_,
        DynamicConfigManager_,
        MemoryTracker_);

    const IChunkStoreHostPtr ChunkStoreHost_ = New<TFakeChunkStoreHost>();
    const IBlobReaderCachePtr BlobReaderCache_ = CreateBlobReaderCache(
        Config_,
        DynamicConfigManager_,
        ChunkMetaManager_);

    const TChunkReaderSweeperPtr ChunkReaderSweeper_ = New<TChunkReaderSweeper>(
        DynamicConfigManager_,
        ActionQueue_->GetInvoker());
    const IJournalDispatcherPtr JournalDispatcher_ = CreateJournalDispatcher(
        Config_,
        DynamicConfigManager_);

    const TChunkContextPtr ChunkContext_ = New<TChunkContext>(TChunkContext{
        .ChunkMetaManager = ChunkMetaManager_,

        .StorageHeavyInvoker = CreatePrioritizedInvoker(ActionQueue_->GetInvoker()),
        .StorageLightInvoker = ActionQueue_->GetInvoker(),
        .DataNodeConfig = Config_,

        .ChunkReaderSweeper = ChunkReaderSweeper_,
        .JournalDispatcher = JournalDispatcher_,
        .BlobReaderCache = BlobReaderCache_,
    });

    TChunkStorePtr ChunkStore_;

    void Start()
    {
        auto locationConfig = New<TStoreLocationConfig>();
        locationConfig->Path = GetOutputPath() / ::testing::UnitTest::GetInstance()->current_test_info()->name() / "store";
        locationConfig->Postprocess();

        Config_->StoreLocations.push_back(locationConfig);
        Config_->Postprocess();

        DynamicConfig_->Postprocess();

        ChunkStore_ = New<TChunkStore>(
            Config_,
            DynamicConfigManager_,
            ActionQueue_->GetInvoker(),
            ChunkContext_,
            ChunkStoreHost_);

        BIND([&] {
            ChunkStore_->Initialize();
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run()
            .Get();
    }

    void Stop()
    {
        BIND([&] {
            ChunkStore_->Shutdown();
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run()
            .Get();
    }

    void SetUp() override
    {
        Start();
    }

    void TearDown() override
    {
        Stop();
    }
};

TEST_F(TJournalTest, Write)
{
    auto journalManager = ChunkStore_->Locations()[0]->GetJournalManager();

    for (bool multiplexed : {true, false}) {
        auto journalId = MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1));

        auto changelog = journalManager->CreateChangelog(journalId, multiplexed, TWorkloadDescriptor{})
            .Get()
            .ValueOrThrow();

        auto r0 = TSharedRef::FromString("r0");
        auto r1 = TSharedRef::FromString("r1");

        changelog->Append({r0, r1})
            .Get()
            .ThrowOnError();

        changelog->Close()
            .Get()
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
