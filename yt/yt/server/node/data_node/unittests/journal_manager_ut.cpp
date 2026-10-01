#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/chunk_detail.h>
#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/blob_reader_cache.h>
#include <yt/yt/server/node/data_node/chunk_reader_sweeper.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/chunk_meta_manager.h>
#include <yt/yt/server/node/data_node/journal_chunk.h>
#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/journal_manager.h>
#include <yt/yt/server/node/data_node/private.h>

#include <yt/yt/server/lib/hydra/file_changelog.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/chunk_fragment.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/testing/common/env.h>

#include <util/system/file.h>

#include <barrier>

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

    INodeMemoryTrackerPtr GetNodeMemoryUsageTracker() override
    {
        return MemoryUsageTracker_;
    }

    void CancelLocationSessions(const TChunkLocationPtr& /*location*/) override
    { }

    bool CanPassSessionOutOfTurn(TChunkId /*chunkId*/) override
    {
        return false;
    }

    void RemoveChunkFromCache(TChunkId /*chunkId*/) override
    { }

    const TFairShareHierarchicalSchedulerPtr<std::string>& GetFairShareHierarchicalScheduler()  override
    {
        return FairShareHierarchicalScheduler_;
    }

    const NIO::IHugePageManagerPtr& GetHugePageManager()  override
    {
        return HugePageManager_;
    }

    THashSet<NObjectClient::TCellTag> GetMasterCellTags() const override
    {
        return {};
    }

private:
    const INodeMemoryTrackerPtr MemoryUsageTracker_ = CreateNodeMemoryTracker(1_GBs, New<TNodeMemoryTrackerConfig>());
    const TFairShareHierarchicalSchedulerPtr<std::string> FairShareHierarchicalScheduler_ = nullptr;
    const NIO::IHugePageManagerPtr HugePageManager_ = nullptr;
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

    const INodeMemoryTrackerPtr MemoryTracker_ = CreateNodeMemoryTracker(1_GBs, New<TNodeMemoryTrackerConfig>());
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

        WaitFor(BIND([&] {
            ChunkStore_->Initialize();
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run())
            .ThrowOnError();
    }

    void Stop()
    {
        WaitFor(BIND([&] {
            ChunkStore_->Shutdown();
        })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run())
            .ThrowOnError();
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

        auto changelog = WaitForFast(journalManager->CreateChangelog(journalId, multiplexed, TWorkloadDescriptor{}))
            .ValueOrThrow();

        auto r0 = TSharedRef::FromString(std::string("r0"));
        auto r1 = TSharedRef::FromString(std::string("r1"));

        WaitForFast(changelog->Append({r0, r1}))
            .ThrowOnError();

        WaitForFast(changelog->Close())
            .ThrowOnError();
    }
}

TEST_F(TJournalTest, SealReplicaAfterRecoveringOrphanedSeal)
{
    auto location = ChunkStore_->Locations().front();
    NNode::TChunkDescriptor descriptor;
    descriptor.Id = MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1));

    // An interrupted replica deletion left only the seal on disk.
    TFile(TString(location->GetChunkPath(descriptor.Id) + "." + SealedFlagExtension), CreateNew).Close();
    WaitFor(BIND([&] {
        ChunkStore_->Shutdown();
        ChunkStore_->Initialize();
    })
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run())
        .ThrowOnError();

    location = ChunkStore_->Locations().front();
    auto journalManager = location->GetJournalManager();
    auto changelog = WaitFor(journalManager->CreateChangelog(descriptor.Id, /*enableMultiplexing*/ false, {}))
        .ValueOrThrow();
    WaitFor(changelog->Close())
        .ThrowOnError();

    auto chunk = New<TJournalChunk>(ChunkContext_, location, descriptor);
    auto sealResult = WaitFor(journalManager->SealChangelog(chunk));
    EXPECT_TRUE(location->IsEnabled());
    EXPECT_TRUE(sealResult.IsOK()) << ToString(sealResult);
}

////////////////////////////////////////////////////////////////////////////////

class TMockJournalDispatcher
    : public IJournalDispatcher
{
public:
    MOCK_METHOD(TFuture<NHydra::IFileChangelogPtr>, OpenJournal, (const TStoreLocationPtr&, TChunkId), (override));
    MOCK_METHOD(TFuture<NHydra::IFileChangelogPtr>, CreateJournal, (const TStoreLocationPtr&, TChunkId, bool, const TWorkloadDescriptor&), (override));
    MOCK_METHOD(TFuture<void>, RemoveJournal, (const TJournalChunkPtr&, bool), (override));
    MOCK_METHOD(bool, IsJournalSealed, (const TStoreLocationPtr&, TChunkId), (const, override));
    MOCK_METHOD(TFuture<void>, SealJournal, (TJournalChunkPtr), (override));
};

class TMockBlobReaderCache
    : public IBlobReaderCache
{
public:
    MOCK_METHOD(NIO::TChunkFileReaderPtr, GetReader, (const TBlobChunkBasePtr&), (override));
    MOCK_METHOD(void, EvictReader, (TBlobChunkBase*), (override));
};

class TChunkFragmentPreparationTest
    : public TJournalTest
{
protected:
    static constexpr int ConcurrentPrepareCount = 16;

    const IThreadPoolPtr ThreadPool_ = CreateThreadPool(ConcurrentPrepareCount, "ChunkPrepareTest");
    const NIO::TChunkFragmentDescriptor Fragment_ = {
        .Length = 1,
        .BlockIndex = 0,
        .BlockOffset = 0,
    };

    std::vector<TWeakPtr<IChunk>> Chunks_;

    void SetUp() override
    {
        DynamicConfig_->DataNode->ChunkReaderRetentionTimeout = TDuration::Zero();
        TJournalTest::SetUp();
    }

    void TearDown() override
    {
        ThreadPool_->Shutdown();
        // The sweeper retains chunks, and hence the context and its mocks, until
        // their last scheduled sweep. Drain them before shutting down the store.
        WaitForPredicate([&] {
            return std::all_of(Chunks_.begin(), Chunks_.end(), [] (const auto& chunk) {
                return chunk.IsExpired();
            });
        });
        TJournalTest::TearDown();
    }

    template <class TChunk>
    TIntrusivePtr<TChunk> CreateChunk(const TChunkContextPtr& context, const NNode::TChunkDescriptor& descriptor)
    {
        auto chunk = New<TChunk>(context, ChunkStore_->Locations().front(), descriptor);
        Chunks_.push_back(chunk);
        return chunk;
    }

    std::vector<TFuture<void>> PrepareConcurrently(const IChunkPtr& chunk)
    {
        std::barrier barrier(ConcurrentPrepareCount);
        std::vector<TFuture<void>> preparations(ConcurrentPrepareCount);
        std::vector<TFuture<void>> calls;
        for (int index = 0; index < ConcurrentPrepareCount; ++index) {
            calls.push_back(BIND([&, index] {
                barrier.arrive_and_wait();
                preparations[index] = chunk->PrepareToReadChunkFragments({}, false);
            }).AsyncVia(ThreadPool_->GetInvoker()).Run());
        }
        auto results = WaitFor(AllSet(calls)).ValueOrThrow();
        for (const auto& result : results) {
            result.ThrowOnError();
        }
        return preparations;
    }

    NNode::TChunkDescriptor WriteBlobChunk()
    {
        auto location = ChunkStore_->Locations().front();
        NNode::TChunkDescriptor descriptor;
        descriptor.Id = MakeRandomId(NObjectClient::EObjectType::Chunk, NObjectClient::TCellTag(1));
        auto writer = New<NIO::TChunkFileWriter>(
            location->GetIOEngine(),
            descriptor.Id,
            location->GetChunkPath(descriptor.Id));
        WaitFor(writer->Open()).ThrowOnError();
        writer->WriteBlock({}, {}, TBlock(TSharedRef::FromString(std::string("fragment"))));
        WaitFor(writer->GetReadyEvent()).ThrowOnError();
        WaitFor(writer->Close({}, {}, New<TDeferredChunkMeta>())).ThrowOnError();
        return descriptor;
    }

    NIO::TChunkFileReaderPtr CreatePreparedReader(const NNode::TChunkDescriptor& descriptor)
    {
        auto location = ChunkStore_->Locations().front();
        auto reader = New<NIO::TChunkFileReader>(
            location->GetIOEngine(),
            descriptor.Id,
            location->GetChunkPath(descriptor.Id));
        WaitFor(reader->PrepareToReadChunkFragments({}, false)).ThrowOnError();
        return reader;
    }
};

TEST_F(TChunkFragmentPreparationTest, ConcurrentPrepareToReadChunkFragments)
{
    auto location = ChunkStore_->Locations().front();
    NNode::TChunkDescriptor descriptor;
    descriptor.Id = MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1));
    auto changelog = WaitFor(location->GetJournalManager()->CreateChangelog(descriptor.Id, false, {}))
        .ValueOrThrow();
    WaitFor(changelog->Append({TSharedRef::FromString(std::string("fragment"))})).ThrowOnError();
    WaitFor(changelog->Flush()).ThrowOnError();

    auto dispatcher = New<TMockJournalDispatcher>();
    auto context = New<TChunkContext>(*ChunkContext_);
    context->JournalDispatcher = dispatcher;

    for (int iteration = 0; iteration < 100; ++iteration) {
        SCOPED_TRACE(iteration);
        auto chunk = CreateChunk<TJournalChunk>(context, descriptor);
        auto guard = TChunkReadGuard::Acquire(chunk);
        auto openPromise = NewPromise<NHydra::IFileChangelogPtr>();
        // Extra calls must report a failed expectation, not return a null future.
        ON_CALL(*dispatcher, OpenJournal(_, _)).WillByDefault(Return(openPromise.ToFuture()));
        EXPECT_CALL(*dispatcher, OpenJournal(location, descriptor.Id)).Times(1);

        auto preparations = PrepareConcurrently(chunk);
        for (const auto& preparation : preparations) {
            EXPECT_EQ(preparations.front(), preparation);
            EXPECT_FALSE(preparation.IsSet());
        }
        openPromise.Set(changelog);
        WaitFor(AllSucceeded(preparations)).ThrowOnError();
        EXPECT_TRUE(chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
        EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
        EXPECT_TRUE(Mock::VerifyAndClear(dispatcher.Get()));
    }

    WaitFor(changelog->Close()).ThrowOnError();
}

TEST_F(TChunkFragmentPreparationTest, PrepareReusesWeakChangelog)
{
    auto location = ChunkStore_->Locations().front();
    NNode::TChunkDescriptor descriptor;
    descriptor.Id = MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1));
    auto changelog = WaitFor(location->GetJournalManager()->CreateChangelog(descriptor.Id, false, {}))
        .ValueOrThrow();
    WaitFor(changelog->Append({TSharedRef::FromString(std::string("fragment"))})).ThrowOnError();
    WaitFor(changelog->Flush()).ThrowOnError();

    auto dispatcher = New<TMockJournalDispatcher>();
    auto context = New<TChunkContext>(*ChunkContext_);
    context->JournalDispatcher = dispatcher;
    auto chunk = CreateChunk<TJournalChunk>(context, descriptor);
    EXPECT_CALL(*dispatcher, OpenJournal(location, descriptor.Id))
        .WillOnce(Return(MakeFuture(changelog)));
    {
        auto guard = TChunkReadGuard::Acquire(chunk);
        WaitFor(chunk->PrepareToReadChunkFragments({}, false)).ThrowOnError();
    }
    EXPECT_TRUE(Mock::VerifyAndClear(dispatcher.Get()));
    WaitForPredicate([&] { return changelog->GetRefCount() == 1; });

    ON_CALL(*dispatcher, OpenJournal(_, _)).WillByDefault(Return(MakeFuture(changelog)));
    EXPECT_CALL(*dispatcher, OpenJournal(_, _)).Times(0);
    {
        auto guard = TChunkReadGuard::Acquire(chunk);
        auto preparation = chunk->PrepareToReadChunkFragments({}, false);
        EXPECT_FALSE(preparation);
        if (preparation) {
            WaitFor(preparation).ThrowOnError();
        }
        EXPECT_TRUE(chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
    }
    EXPECT_TRUE(Mock::VerifyAndClear(dispatcher.Get()));
    WaitFor(changelog->Close()).ThrowOnError();
}

TEST_F(TChunkFragmentPreparationTest, ReadBlockRangeAndPreparePreservePublishedChangelog)
{
    auto location = ChunkStore_->Locations().front();
    NNode::TChunkDescriptor descriptor;
    descriptor.Id = MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1));
    auto createChangelog = [&] (TChunkId id, std::string payload) {
        auto changelog = WaitFor(location->GetJournalManager()->CreateChangelog(id, false, {}))
            .ValueOrThrow();
        WaitFor(changelog->Append({TSharedRef::FromString(std::move(payload))})).ThrowOnError();
        WaitFor(changelog->Flush()).ThrowOnError();
        return changelog;
    };

    // The real dispatcher coalesces opens. Distinct results here make a forbidden
    // reassignment observable without a sanitizer, even when the writes are serialized.
    auto readChangelog = createChangelog(descriptor.Id, "block reader");
    auto prepareChangelog = createChangelog(
        MakeRandomId(NObjectClient::EObjectType::JournalChunk, NObjectClient::TCellTag(1)),
        "fragment reader");
    auto readHandle = readChangelog->MakeChunkFragmentReadRequest(Fragment_).Handle;
    auto prepareHandle = prepareChangelog->MakeChunkFragmentReadRequest(Fragment_).Handle;
    ASSERT_NE(readHandle, prepareHandle);

    auto dispatcher = New<TMockJournalDispatcher>();
    auto context = New<TChunkContext>(*ChunkContext_);
    context->JournalDispatcher = dispatcher;
    context->DynamicConfigManager = DynamicConfigManager_;

    for (bool readFirst : {true, false}) {
        SCOPED_TRACE(readFirst);
        auto chunk = CreateChunk<TJournalChunk>(context, descriptor);
        auto guard = TChunkReadGuard::Acquire(chunk);
        auto readOpenStarted = NewPromise<void>();
        auto readOpenPromise = NewPromise<NHydra::IFileChangelogPtr>();
        auto prepareOpenPromise = NewPromise<NHydra::IFileChangelogPtr>();
        EXPECT_CALL(*dispatcher, OpenJournal(location, descriptor.Id))
            .WillOnce([&] (const TStoreLocationPtr&, TChunkId) {
                readOpenStarted.Set();
                return readOpenPromise.ToFuture();
            })
            .WillOnce(Return(prepareOpenPromise.ToFuture()));

        // Enter GetChangelog's slow path before starting fragment preparation.
        auto readFuture = chunk->ReadBlockRange(0, 1, {});
        WaitFor(readOpenStarted.ToFuture().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
        auto prepareFuture = chunk->PrepareToReadChunkFragments({}, false);
        ASSERT_TRUE(prepareFuture);
        EXPECT_FALSE(readFuture.IsSet());
        EXPECT_FALSE(prepareFuture.IsSet());

        if (readFirst) {
            readOpenPromise.Set(readChangelog);
            WaitFor(readFuture).ThrowOnError();
        } else {
            prepareOpenPromise.Set(prepareChangelog);
            WaitFor(prepareFuture).ThrowOnError();
        }

        auto expectedHandle = readFirst ? readHandle : prepareHandle;
        EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
        EXPECT_EQ(expectedHandle, chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);

        if (readFirst) {
            prepareOpenPromise.Set(prepareChangelog);
        } else {
            readOpenPromise.Set(readChangelog);
        }
        WaitFor(prepareFuture).ThrowOnError();
        auto blocks = WaitFor(readFuture).ValueOrThrow();
        EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
        EXPECT_EQ(expectedHandle, chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
        ASSERT_EQ(1u, blocks.size());
        EXPECT_EQ(TStringBuf(readFirst ? "block reader" : "fragment reader"), blocks[0].Data.ToStringBuf());
        EXPECT_TRUE(Mock::VerifyAndClear(dispatcher.Get()));
    }

    WaitFor(readChangelog->Close()).ThrowOnError();
    WaitFor(prepareChangelog->Close()).ThrowOnError();
}

TEST_F(TChunkFragmentPreparationTest, SlowPrepareDoesNotReplacePublishedReader)
{
    auto descriptor = WriteBlobChunk();
    auto readerA = CreatePreparedReader(descriptor);
    auto readerB = CreatePreparedReader(descriptor);
    auto handleA = readerA->MakeChunkFragmentReadRequest(Fragment_, false).Handle;
    auto handleB = readerB->MakeChunkFragmentReadRequest(Fragment_, false).Handle;
    ASSERT_NE(handleA, handleB);

    auto cache = New<TMockBlobReaderCache>();
    auto context = New<TChunkContext>(*ChunkContext_);
    context->BlobReaderCache = cache;
    auto chunk = CreateChunk<TStoredBlobChunk>(context, descriptor);
    auto guard = TChunkReadGuard::Acquire(chunk);
    auto cacheEntered = NewPromise<void>();
    auto resumeCache = NewPromise<void>();
    EXPECT_CALL(*cache, GetReader(_))
        .WillOnce([&] (const TBlobChunkBasePtr&) {
            cacheEntered.Set();
            WaitFor(resumeCache.ToFuture().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
            return readerA;
        })
        .WillOnce(Return(readerB));

    auto slowCall = BIND([&] {
        EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
    }).AsyncVia(ThreadPool_->GetInvoker()).Run();
    WaitFor(cacheEntered.ToFuture().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
    EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
    EXPECT_EQ(handleB, chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
    resumeCache.Set();
    WaitFor(slowCall).ThrowOnError();
    EXPECT_EQ(handleB, chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
    EXPECT_TRUE(Mock::VerifyAndClear(cache.Get()));
}

TEST_F(TChunkFragmentPreparationTest, ConcurrentPrepareFromCachedWeakReader)
{
    auto descriptor = WriteBlobChunk();
    auto reader = CreatePreparedReader(descriptor);
    auto weakReader = MakeWeak(reader);
    auto expectedHandle = reader->MakeChunkFragmentReadRequest(Fragment_, false).Handle;
    auto cache = New<TMockBlobReaderCache>();
    auto context = New<TChunkContext>(*ChunkContext_);
    context->BlobReaderCache = cache;

    int iterationCount = 100;
    std::vector<TStoredBlobChunkPtr> chunks;
    EXPECT_CALL(*cache, GetReader(_))
        .Times(iterationCount)
        .WillRepeatedly([&] (const TBlobChunkBasePtr&) { return reader; });
    for (int iteration = 0; iteration < iterationCount; ++iteration) {
        auto chunk = CreateChunk<TStoredBlobChunk>(context, descriptor);
        auto guard = TChunkReadGuard::Acquire(chunk);
        EXPECT_FALSE(chunk->PrepareToReadChunkFragments({}, false));
        chunks.push_back(std::move(chunk));
    }
    EXPECT_TRUE(Mock::VerifyAndClear(cache.Get()));
    // All chunks must have released their strong reader before testing weak promotion.
    WaitForPredicate([&] { return reader->GetRefCount() == 1; });

    ON_CALL(*cache, GetReader(_))
        .WillByDefault([&] (const TBlobChunkBasePtr&) { return reader; });
    EXPECT_CALL(*cache, GetReader(_)).Times(0);
    for (const auto& chunk : chunks) {
        auto guard = TChunkReadGuard::Acquire(chunk);
        auto preparations = PrepareConcurrently(chunk);
        for (const auto& preparation : preparations) {
            EXPECT_FALSE(preparation);
        }
        EXPECT_EQ(expectedHandle, chunk->MakeChunkFragmentReadRequest(Fragment_, false).Handle);
    }
    EXPECT_TRUE(Mock::VerifyAndClear(cache.Get()));
    chunks.clear();
    reader.Reset();
    // Also catches leaked intrusive references from concurrent weak-reader promotion.
    WaitForPredicate([&] { return weakReader.IsExpired(); });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
