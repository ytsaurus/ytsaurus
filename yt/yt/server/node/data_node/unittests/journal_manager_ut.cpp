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

#include <yt/yt/server/lib/hydra_common/changelog.h>

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

DECLARE_REFCOUNTED_STRUCT(TFakeChunkStoreHost)

struct TFakeChunkStoreHost
    : public IChunkStoreHost
{
    void ScheduleMasterHeartbeat() override
    { }

    NObjectClient::TCellId GetCellId() override
    {
        return TGuid::FromString("1-2-3-4");
    }

    void SubscribePopulateAlerts(TCallback<void(std::vector<TError>*)> ) override
    { }

    NClusterNode::TMasterEpoch GetMasterEpoch() override
    {
        return 1;
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeChunkStoreHost)

////////////////////////////////////////////////////////////////////////////////

class TJournalTest
    : public ::testing::Test
{
public:
    IBlockCachePtr BlockCache = GetNullBlockCache();
    TActionQueuePtr Thread = New<TActionQueue>("JournalTest");

    TDataNodeConfigPtr Config = New<TDataNodeConfig>();
    TClusterNodeDynamicConfigPtr DynamicConfig = New<TClusterNodeDynamicConfig>();
    TClusterNodeDynamicConfigManagerPtr DynamicConfigManager = New<TClusterNodeDynamicConfigManager>(DynamicConfig);

    INodeMemoryTrackerPtr MemoryTracker = CreateNodeMemoryTracker(
        1_GB,
        std::vector<std::pair<EMemoryCategory, i64>>{},
        Logger,
        NProfiling::TProfiler{});
    IChunkMetaManagerPtr ChunkMetaManager = CreateChunkMetaManager(
        Config,
        DynamicConfigManager,
        MemoryTracker);

    IChunkStoreHostPtr ChunkStoreHost = New<TFakeChunkStoreHost>();
    IBlobReaderCachePtr BlobReaderCache = CreateBlobReaderCache(
        Config,
        DynamicConfigManager,
        ChunkMetaManager);

    TChunkReaderSweeperPtr ChunkReaderSweeper = New<TChunkReaderSweeper>(
        DynamicConfigManager,
        Thread->GetInvoker());
    IJournalDispatcherPtr JournalDispatcher = CreateJournalDispatcher(
        Config,
        DynamicConfigManager);

    TChunkContextPtr ChunkContext = New<TChunkContext>(TChunkContext{
        .ChunkMetaManager = ChunkMetaManager,

        .StorageHeavyInvoker = CreatePrioritizedInvoker(Thread->GetInvoker()),
        .StorageLightInvoker = Thread->GetInvoker(),
        .DataNodeConfig = Config,

        .ChunkReaderSweeper = ChunkReaderSweeper,
        .JournalDispatcher = JournalDispatcher,
        .BlobReaderCache = BlobReaderCache,
    });

    TChunkStorePtr ChunkStore;

    void Start()
    {
        auto locationConfig = New<TStoreLocationConfig>();
        locationConfig->Path = GetOutputPath() / ::testing::UnitTest::GetInstance()->current_test_info()->name() / "store";

        Config->StoreLocations.push_back(locationConfig);

        ChunkStore = New<TChunkStore>(
            Config,
            DynamicConfigManager,
            Thread->GetInvoker(),
            ChunkContext,
            ChunkStoreHost);

        BIND([&] {
            ChunkStore->Initialize();
        })
            .AsyncVia(Thread->GetInvoker())
            .Run()
            .Get();
    }

    void Stop()
    {
        BIND([&] {
            ChunkStore->Shutdown();
        })
            .AsyncVia(Thread->GetInvoker())
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
    auto journalManager = ChunkStore->Locations()[0]->GetJournalManager();

    for (bool multiplexed : {true, false}) {
        auto journalId = TGuid::Create();

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
