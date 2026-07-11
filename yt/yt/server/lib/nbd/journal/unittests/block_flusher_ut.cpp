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
    TFuture<std::vector<TStoredBlockId>> WriteBlocks(TRange<TSharedRef> blocks) override
    {
        auto guard = Guard(Lock_);
        ++WriteCallCount_;
        if (Failing_) {
            return MakeFuture<std::vector<TStoredBlockId>>(TError("Mock store is failing"));
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
        const NChunkClient::TClientChunkReadOptions& /*options*/) override
    {
        YT_ABORT();
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

    void SetUp() override
    {
        Queue_ = New<TActionQueue>("FlusherTest");
        Store_ = New<TMockBlockStore>();
        Config_ = New<TJournalBlockFlusherConfig>();
        Observer_ = New<TFlushObserver>();
    }

    void TearDown() override
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
        Flusher_->SubscribeFailed(BIND(&TFlushObserver::OnFailed, Observer_));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
