#include <yt/yt/server/lib/nbd/journal/block_flusher.h>
#include <yt/yt/server/lib/nbd/journal/block_store.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/dirty_block_pool.h>
#include <yt/yt/server/lib/nbd/journal/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

#include <optional>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("FlusherTest");

////////////////////////////////////////////////////////////////////////////////

//! A stand-in store: records what it was handed, returns sequential stored ids, and can be told to
//! fail every write.
class TMockBlockStore
    : public IBlockStore
{
public:
    TFuture<std::vector<TStoredBlockId>> WriteBlocks(TRange<TSharedRef> blocks) final
    {
        auto guard = Guard(Lock_);
        ++WriteCallCount_;
        if (Failing_) {
            auto error = TError("Mock store is failing");
            // Mirror the real store: it is the store, not the flusher, that reports the failure.
            // Fire outside the lock, as a subscriber may re-enter.
            guard.Release();
            Failed_.Fire(error);
            return MakeFuture<std::vector<TStoredBlockId>>(error);
        }
        std::vector<TStoredBlockId> ids;
        ids.reserve(blocks.size());
        for (const auto& block : blocks) {
            WrittenBlocks_.push_back(block);
            ids.push_back(TStoredBlockId(NextStoredId_++));
        }
        return MakeFuture(std::move(ids));
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        TRange<TStoredBlockId> /*blockIds*/,
        EWorkloadCategory /*workloadCategory*/) final
    {
        YT_ABORT();
    }

    TFuture<void> SealChunks(TRange<NChunkClient::TChunkId> /*chunkIds*/) final
    {
        YT_ABORT();
    }

    std::vector<TStoredBlockRef> GetBlockRefs(TRange<TStoredBlockId> /*blockIds*/) final
    {
        YT_ABORT();
    }

    TFuture<void> BeginRestoreBlocks() final
    {
        YT_ABORT();
    }

    TFuture<std::vector<TStoredBlockId>> RestoreBlocks(std::vector<TSnapshotBlock> /*snapshotBlocks*/) final
    {
        YT_ABORT();
    }

    TFuture<void> EndRestoreBlocks(const TChunkBlockCounts& /*chunkBlockCounts*/) final
    {
        YT_ABORT();
    }

    void ReleaseBlock(TStoredBlockId /*blockId*/) final
    { }

    void BeginSnapshot() final
    { }

    void EndSnapshot() final
    { }

    void Start() final
    { }

    void Stop() final
    {
        YT_ABORT();
    }

    void SubscribeFailed(const TCallback<void(const TError&)>& callback) final
    {
        Failed_.Subscribe(callback);
    }

    void UnsubscribeFailed(const TCallback<void(const TError&)>& callback) final
    {
        Failed_.Unsubscribe(callback);
    }

    std::vector<TChunkInfo> GetChunkInfos() final
    {
        return {};
    }

    void SetFailing(bool failing)
    {
        auto guard = Guard(Lock_);
        Failing_ = failing;
    }

    int GetWriteCallCount() const
    {
        auto guard = Guard(Lock_);
        return WriteCallCount_;
    }

    int GetWrittenBlockCount() const
    {
        auto guard = Guard(Lock_);
        return std::ssize(WrittenBlocks_);
    }

private:
    TSingleShotCallbackList<void(const TError&)> Failed_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    bool Failing_ = false;
    int WriteCallCount_ = 0;
    ui64 NextStoredId_ = 1;
    std::vector<TSharedRef> WrittenBlocks_;
};

DEFINE_REFCOUNTED_TYPE(TMockBlockStore)

using TMockBlockStorePtr = TIntrusivePtr<TMockBlockStore>;

////////////////////////////////////////////////////////////////////////////////

//! Collects the flusher's signals for assertions.
class TFlushObserver
    : public TRefCounted
{
public:
    void OnBlockFlushed(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId)
    {
        auto guard = Guard(Lock_);
        Flushed_.emplace_back(block->BlockIndex, storedBlockId);
    }

    void OnFailed(const TError& error)
    {
        auto guard = Guard(Lock_);
        Failure_ = error;
    }

    std::vector<std::pair<int, TStoredBlockId>> GetFlushed() const
    {
        auto guard = Guard(Lock_);
        return Flushed_;
    }

    int GetFlushedCount() const
    {
        auto guard = Guard(Lock_);
        return std::ssize(Flushed_);
    }

    bool HasFailed() const
    {
        auto guard = Guard(Lock_);
        return Failure_.has_value();
    }

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::vector<std::pair<int, TStoredBlockId>> Flushed_;
    std::optional<TError> Failure_;
};

using TFlushObserverPtr = TIntrusivePtr<TFlushObserver>;

////////////////////////////////////////////////////////////////////////////////

bool WaitUntil(const std::function<bool()>& predicate)
{
    auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (TInstant::Now() < deadline) {
        if (predicate()) {
            return true;
        }
        Sleep(TDuration::MilliSeconds(5));
    }
    return predicate();
}

////////////////////////////////////////////////////////////////////////////////

class TBlockFlusherTest
    : public ::testing::Test
{
protected:
    TActionQueuePtr Queue_;
    TMockBlockStorePtr Store_;
    TJournalBlockFlusherConfigPtr Config_;
    TFlushObserverPtr Observer_;
    IDirtyBlockPoolPtr Pool_;
    IBlockFlusherPtr Flusher_;

    void SetUp() final
    {
        Queue_ = New<TActionQueue>("FlusherTest");
        Store_ = New<TMockBlockStore>();
        Config_ = New<TJournalBlockFlusherConfig>();
        Observer_ = New<TFlushObserver>();
    }

    void TearDown() final
    {
        if (Flusher_) {
            Flusher_->Stop();
            Flusher_.Reset();
        }
        if (Queue_) {
            Queue_->Shutdown();
            Queue_.Reset();
        }
    }

    //! Builds the pool (of |poolCapacity| blocks) and the flusher (draining down to
    //! |threshold| * capacity), wiring the observer to its signals.
    void CreateFlusher(int poolCapacity, double threshold)
    {
        Pool_ = CreateDirtyBlockPool(poolCapacity);
        Config_->DirtyFractionThreshold = threshold;
        Flusher_ = CreateBlockFlusher(Config_, Pool_, Store_, Queue_->GetInvoker(), Logger);
        Flusher_->SubscribeBlockFlushed(BIND(&TFlushObserver::OnBlockFlushed, Observer_));
        Store_->SubscribeFailed(BIND(&TFlushObserver::OnFailed, Observer_));
    }

    //! Synchronously puts |count| fresh blocks with indices [baseIndex, baseIndex + count).
    void PutBlocks(int count, int baseIndex = 0)
    {
        std::vector<TDirtyBlockPtr> blocks;
        blocks.reserve(count);
        for (int index = 0; index < count; ++index) {
            blocks.push_back(New<TDirtyBlock>(
                baseIndex + index,
                TSharedRef::FromString(TString("block"))));
        }
        auto future = Pool_->Put(TRange(blocks));
        // The pool has room, so the put resolves synchronously and in full.
        ASSERT_TRUE(future.IsSet());
        EXPECT_EQ(std::ssize(future.TryGet()->ValueOrThrow()), count);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBlockFlusherTest, FlushesExcessAndReportsStoredIds)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25); // resident target = 4
    PutBlocks(16);
    ASSERT_EQ(Pool_->GetSize(), 16);

    Flusher_->Start();
    Flusher_->RequestFlush();

    // The 12 oldest blocks are flushed; the target stays resident.
    ASSERT_TRUE(WaitUntil([&] { return Pool_->GetSize() == 4; }));

    EXPECT_EQ(Store_->GetWrittenBlockCount(), 12);
    EXPECT_EQ(Observer_->GetFlushedCount(), 12);
    EXPECT_FALSE(Observer_->HasFailed());

    // FIFO order, each block reported with the id the store handed back.
    auto flushed = Observer_->GetFlushed();
    for (int index = 0; index < 12; ++index) {
        EXPECT_EQ(flushed[index].first, index);
        EXPECT_EQ(flushed[index].second, TStoredBlockId(index + 1));
    }
}

TEST_F(TBlockFlusherTest, DoesNotFlushBelowTarget)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.5); // resident target = 8
    PutBlocks(4); // below the target

    Flusher_->Start();
    Flusher_->RequestFlush();

    // Nothing should ever be flushed; wait a bit and confirm the pool is untouched.
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(Store_->GetWriteCallCount(), 0);
    EXPECT_EQ(Pool_->GetSize(), 4);
    EXPECT_EQ(Observer_->GetFlushedCount(), 0);
}

TEST_F(TBlockFlusherTest, FlushesNewBlocksAcrossRounds)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25); // resident target = 4
    PutBlocks(16, /*baseIndex*/ 0);

    Flusher_->Start();
    Flusher_->RequestFlush();
    ASSERT_TRUE(WaitUntil([&] { return Pool_->GetSize() == 4; })); // flushed indices 0..11

    // Refill above the target and flush again; the next-oldest blocks go out in order.
    PutBlocks(8, /*baseIndex*/ 16);
    ASSERT_EQ(Pool_->GetSize(), 12);
    Flusher_->RequestFlush();
    ASSERT_TRUE(WaitUntil([&] { return Observer_->GetFlushedCount() == 20; })); // + indices 12..19

    EXPECT_EQ(Pool_->GetSize(), 4);
    auto flushed = Observer_->GetFlushed();
    for (int index = 0; index < 20; ++index) {
        EXPECT_EQ(flushed[index].first, index);
        EXPECT_EQ(flushed[index].second, TStoredBlockId(index + 1));
    }
}

TEST_F(TBlockFlusherTest, RequestFlushBarrierDrainsBelowResidentFraction)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.5); // resident target = 8
    PutBlocks(12);
    ASSERT_EQ(Pool_->GetSize(), 12);

    Flusher_->Start();

    // A plain flush keeps the resident fraction: only the 4 above the target of 8 go out.
    Flusher_->RequestFlush();
    ASSERT_TRUE(WaitUntil([&] { return Pool_->GetSize() == 8; }));
    EXPECT_EQ(Observer_->GetFlushedCount(), 4);

    // An eager flush ignores the target and drains everything enqueued so far.
    WaitFor(Flusher_->RequestFlushBarrier())
        .ThrowOnError();
    EXPECT_EQ(Pool_->GetSize(), 0);
    EXPECT_EQ(Observer_->GetFlushedCount(), 12);
    EXPECT_FALSE(Observer_->HasFailed());

    // FIFO order preserved across both rounds.
    auto flushed = Observer_->GetFlushed();
    for (int index = 0; index < 12; ++index) {
        EXPECT_EQ(flushed[index].first, index);
        EXPECT_EQ(flushed[index].second, TStoredBlockId(index + 1));
    }

    // The watermark is spent: the flusher reverts to keeping the resident fraction. Refill below the
    // target and confirm a plain flush leaves it untouched.
    PutBlocks(4, /*baseIndex*/ 12);
    Flusher_->RequestFlush();
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(Pool_->GetSize(), 4);
    EXPECT_EQ(Observer_->GetFlushedCount(), 12);
}

TEST_F(TBlockFlusherTest, RequestFlushBarrierIsBoundedToTheLatchedTail)
{
    CreateFlusher(/*poolCapacity*/ 64, /*threshold*/ 0.25); // resident target = 16
    PutBlocks(8); // below the target: a plain flush would drain nothing

    Flusher_->Start();

    // Eagerly flush the 8 enqueued so far; the pool empties despite being below the resident target.
    WaitFor(Flusher_->RequestFlushBarrier())
        .ThrowOnError();
    EXPECT_EQ(Observer_->GetFlushedCount(), 8);
    EXPECT_EQ(Pool_->GetSize(), 0);

    // Blocks enqueued after the latched tail are not chased by that eager flush: still below the
    // target, they stay resident until an explicit flush.
    PutBlocks(8, /*baseIndex*/ 8);
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(Pool_->GetSize(), 8);
    EXPECT_EQ(Observer_->GetFlushedCount(), 8);
}

TEST_F(TBlockFlusherTest, PersistentFailureFiresFailedAndKeepsBlocks)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25);
    Store_->SetFailing(true);
    PutBlocks(16);

    Flusher_->Start();
    Flusher_->RequestFlush();

    ASSERT_TRUE(WaitUntil([&] { return Observer_->HasFailed(); }));

    // The blocks are neither drained nor reported clean.
    EXPECT_EQ(Pool_->GetSize(), 16);
    EXPECT_EQ(Observer_->GetFlushedCount(), 0);

    // The flusher has given up for good: even once the store recovers it does not resume.
    Store_->SetFailing(false);
    Flusher_->RequestFlush();
    Sleep(TDuration::MilliSeconds(300));
    EXPECT_EQ(Pool_->GetSize(), 16);
    EXPECT_EQ(Observer_->GetFlushedCount(), 0);
}

TEST_F(TBlockFlusherTest, RequestFlushBarrierOnEmptyPoolSucceedsWithoutFlushing)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25);
    Flusher_->Start();

    WaitFor(Flusher_->RequestFlushBarrier())
        .ThrowOnError();
    EXPECT_EQ(Store_->GetWriteCallCount(), 0);
    EXPECT_EQ(Observer_->GetFlushedCount(), 0);
}

TEST_F(TBlockFlusherTest, MultipleFlushBarriersAllResolve)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.5);
    PutBlocks(4);

    auto firstFlushBarrierFuture = Flusher_->RequestFlushBarrier();
    PutBlocks(4, /*baseIndex*/ 4);
    auto secondFlushBarrierFuture = Flusher_->RequestFlushBarrier();

    Flusher_->Start();

    // Both barriers are drained despite the pool never exceeding the resident target of 8.
    WaitFor(firstFlushBarrierFuture)
        .ThrowOnError();
    WaitFor(secondFlushBarrierFuture)
        .ThrowOnError();
    EXPECT_EQ(Observer_->GetFlushedCount(), 8);
    EXPECT_EQ(Pool_->GetSize(), 0);
}

TEST_F(TBlockFlusherTest, RequestFlushBarrierFailsOnFlushFailure)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25);
    Store_->SetFailing(true);
    PutBlocks(16);

    // Requested before the start, so it is pending when the failing flush fails it.
    auto flushBarrierFuture = Flusher_->RequestFlushBarrier();
    Flusher_->Start();

    EXPECT_FALSE(WaitFor(flushBarrierFuture).IsOK());
    EXPECT_EQ(Pool_->GetSize(), 16);

    // A barrier requested after the failure is refused outright.
    EXPECT_FALSE(WaitFor(Flusher_->RequestFlushBarrier()).IsOK());
}

TEST_F(TBlockFlusherTest, FlushFailureReleasesParkedPut)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25);
    Store_->SetFailing(true);
    PutBlocks(16);

    // The pool is full, so this put parks until space frees up -- which the failing flush never does.
    auto extraBlock = New<TDirtyBlock>(16, TSharedRef::FromString(TString("block")));
    auto parkedFuture = Pool_->Put(TRange(&extraBlock, 1));
    ASSERT_FALSE(parkedFuture.IsSet());

    Flusher_->Start();

    EXPECT_FALSE(WaitFor(parkedFuture).IsOK());
}

TEST_F(TBlockFlusherTest, RequestFlushBarrierFailsOnStop)
{
    CreateFlusher(/*poolCapacity*/ 16, /*threshold*/ 0.25);
    PutBlocks(16);

    // Never started, so nothing drains and the barrier stays pending until Stop resolves it.
    auto flushBarrierFuture = Flusher_->RequestFlushBarrier();
    Flusher_->Stop();

    EXPECT_FALSE(WaitFor(flushBarrierFuture).IsOK());

    // A barrier requested after the stop is refused rather than left pending forever.
    EXPECT_FALSE(WaitFor(Flusher_->RequestFlushBarrier()).IsOK());

    // The stop releases parked writers too.
    auto extraBlock = New<TDirtyBlock>(16, TSharedRef::FromString(TString("block")));
    EXPECT_FALSE(WaitFor(Pool_->Put(TRange(&extraBlock, 1))).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
