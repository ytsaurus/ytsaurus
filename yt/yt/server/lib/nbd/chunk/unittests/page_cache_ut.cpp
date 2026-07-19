#include <yt/yt/server/lib/nbd/chunk/page_cache.h>
#include <yt/yt/server/lib/nbd/chunk/chunk_handler.h>
#include <yt/yt/server/lib/nbd/chunk/config.h>
#include <yt/yt/server/lib/nbd/chunk/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

#include <atomic>
#include <optional>
#include <vector>

namespace NYT::NNbd::NChunk {
namespace {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("PageCacheTest");

////////////////////////////////////////////////////////////////////////////////

//! A mock backend implementing IChunkHandler over an in-memory byte buffer.
//!
//! - Read serves bytes from the buffer (zero-initialized beyond written regions).
//! - Write / WriteBatch record bytes into the buffer and log every call so tests
//!   can assert on what reached the backend.
//! - Flush records a call count.
//! - Initialize / Finalize are no-ops that succeed.
//! - Can be put into a failing mode where every Write returns an error.
class TMockChunkHandler
    : public IChunkHandler
{
public:
    explicit TMockChunkHandler(i64 size)
        : Storage_(TSharedMutableRef::Allocate(size, {.InitializeStorage = true}))
    {
    }

    TFuture<void> Initialize() override
    {
        return OKFuture;
    }

    TFuture<void> Finalize() override
    {
        return OKFuture;
    }

    TFuture<void> Flush(const TFlushOptions& /*options*/) override
    {
        auto guard = Guard(Lock_);
        ++FlushCallCount_;
        return OKFuture;
    }

    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& /*options*/) override
    {
        auto guard = Guard(Lock_);
        ++ReadCallCount_;
        YT_VERIFY(offset >= 0);
        YT_VERIFY(length >= 0);
        YT_VERIFY(offset + length <= std::ssize(Storage_));
        auto data = TSharedMutableRef::Allocate(length, {.InitializeStorage = false});
        std::copy(
            Storage_.Begin() + offset,
            Storage_.Begin() + offset + length,
            data.Begin());
        return MakeFuture<TReadResponse>(TReadResponse(TSharedRef(std::move(data)), false));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        auto guard = Guard(Lock_);
        ++WriteCallCount_;
        if (Failing_) {
            return MakeFuture<TWriteResponse>(TError("Mock chunk handler is failing"));
        }
        RecordWrite(offset, data, options.Flush);
        return MakeFuture<TWriteResponse>(TWriteResponse{});
    }

    TFuture<std::vector<TReadResponse>> ReadBatch(
        const std::vector<TReadBatchSubrequest>& /*subrequests*/,
        const TReadOptions& /*options*/) override
    {
        YT_ABORT();
    }

    TFuture<TWriteResponse> WriteBatch(
        const std::vector<TWriteBatchSubrequest>& subrequests,
        const TWriteOptions& options) override
    {
        auto guard = Guard(Lock_);
        ++WriteBatchCallCount_;
        if (Failing_) {
            return MakeFuture<TWriteResponse>(TError("Mock chunk handler is failing"));
        }
        for (const auto& sub : subrequests) {
            RecordWrite(sub.Offset, sub.Data, options.Flush);
        }
        return MakeFuture<TWriteResponse>(TWriteResponse{});
    }

    void SetFailing(bool failing)
    {
        auto guard = Guard(Lock_);
        Failing_ = failing;
    }

    int GetReadCallCount() const
    {
        auto guard = Guard(Lock_);
        return ReadCallCount_;
    }

    int GetWriteCallCount() const
    {
        auto guard = Guard(Lock_);
        return WriteCallCount_;
    }

    int GetWriteBatchCallCount() const
    {
        auto guard = Guard(Lock_);
        return WriteBatchCallCount_;
    }

    int GetFlushCallCount() const
    {
        auto guard = Guard(Lock_);
        return FlushCallCount_;
    }

    //! Total number of WriteBatch subrequests that reached the backend.
    int GetWrittenSubrequestCount() const
    {
        auto guard = Guard(Lock_);
        return WrittenSubrequestCount_;
    }

    //! Number of FUA (Flush=true) writes that reached the backend.
    int GetFuaWriteCount() const
    {
        auto guard = Guard(Lock_);
        return FuaWriteCount_;
    }

    //! Read a slice of the backend storage for assertions.
    TSharedRef ReadStorage(i64 offset, i64 length) const
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(offset >= 0);
        YT_VERIFY(length >= 0);
        YT_VERIFY(offset + length <= std::ssize(Storage_));
        auto data = TSharedMutableRef::Allocate(length, {.InitializeStorage = false});
        std::copy(
            Storage_.Begin() + offset,
            Storage_.Begin() + offset + length,
            data.Begin());
        return TSharedRef(std::move(data));
    }

private:
    void RecordWrite(i64 offset, const TSharedRef& data, bool flush)
    {
        YT_VERIFY(offset >= 0);
        YT_VERIFY(offset + std::ssize(data) <= std::ssize(Storage_));
        std::copy(data.Begin(), data.End(), Storage_.Begin() + offset);
        ++WrittenSubrequestCount_;
        if (flush) {
            ++FuaWriteCount_;
        }
    }

private:
    mutable YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TSharedMutableRef Storage_;
    bool Failing_ = false;
    int ReadCallCount_ = 0;
    int WriteCallCount_ = 0;
    int WriteBatchCallCount_ = 0;
    int FlushCallCount_ = 0;
    int WrittenSubrequestCount_ = 0;
    int FuaWriteCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TMockChunkHandler)

using TMockChunkHandlerPtr = TIntrusivePtr<TMockChunkHandler>;

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

//! Synchronously wait for a future to complete, throwing on error.
template <class T>
T WaitForSync(TFuture<T> future)
{
    auto result = WaitFor(std::move(future));
    result.ThrowOnError();
    return result.Value();
}

//! Specialization for void futures: no value to return.
void WaitForSync(TFuture<void> future)
{
    WaitFor(std::move(future)).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

//! Byte-wise comparison of two TSharedRef / TRef values for EXPECT_TRUE.
bool RefsEqual(const TRef& a, const TRef& b)
{
    return std::ssize(a) == std::ssize(b) &&
        std::equal(a.Begin(), a.End(), b.Begin());
}

////////////////////////////////////////////////////////////////////////////////

class TPageCacheTest
    : public ::testing::Test
{
protected:
    static constexpr i64 PageSize = 4_KB;
    static constexpr i64 Capacity = 64_KB;   // 16 pages
    static constexpr i64 DeviceSize = 1_MB;  // backend buffer size

    TActionQueuePtr Queue_;
    TMockChunkHandlerPtr Backend_;
    TPageCacheConfigPtr Config_;
    IChunkHandlerPtr Cache_;

    void SetUp() override
    {
        Queue_ = New<TActionQueue>("PageCacheTest");
        Backend_ = New<TMockChunkHandler>(DeviceSize);
        Config_ = New<TPageCacheConfig>();
        Config_->PageSize = PageSize;
        Config_->Capacity = Capacity;
        // Disable periodic writeback for deterministic tests; we trigger
        // writeback explicitly via Flush() or by hitting the watermarks.
        Config_->WritebackPeriod = std::nullopt;
        Config_->DirtyDataSoftLimitCapacityFraction = 0.25;   // 16 KB = 4 pages
        Config_->DirtyDataHardLimitCapacityFraction = 0.5;    // 32 KB = 8 pages
        Config_->MaxDirtyDataPerWritebackCapacityFraction = 0.25; // 16 KB = 4 pages
        Config_->MaxDirtyDataPerWrite = 16_KB;
        Config_->MaxInflightWriteRequests = 4;
    }

    void TearDown() override
    {
        if (Cache_) {
            WaitForSync(Cache_->Finalize());
            Cache_.Reset();
        }
        if (Queue_) {
            Queue_->Shutdown();
            Queue_.Reset();
        }
    }

    void CreateCache()
    {
        Cache_ = New<TPageCache>(Config_, Backend_, Queue_->GetInvoker(), Logger);
        WaitForSync(Cache_->Initialize());
    }

    //! Build a filled buffer of |length| bytes with a recognizable pattern.
    static TSharedRef MakeData(i64 length, ui8 fill)
    {
        auto data = TSharedMutableRef::Allocate(length, {.InitializeStorage = false});
        std::fill(data.Begin(), data.End(), fill);
        return data;
    }

    TSharedRef Read(i64 offset, i64 length)
    {
        auto response = WaitForSync(Cache_->Read(offset, length, {}));
        return response.Data;
    }

    void Write(i64 offset, const TSharedRef& data, bool flush = false)
    {
        WaitForSync(Cache_->Write(offset, data, {.Flush = flush}));
    }

    void Flush()
    {
        WaitForSync(Cache_->Flush({}));
    }
};

////////////////////////////////////////////////////////////////////////////////

// Write then read returns the written bytes (read-your-writes consistency).
TEST_F(TPageCacheTest, WriteThenReadReturnsWrittenData)
{
    CreateCache();

    auto data = MakeData(PageSize, 0xAB);
    Write(0, data);

    auto result = Read(0, PageSize);
    EXPECT_TRUE(RefsEqual(result, data));
}

// A read of an unwritten region goes to the backend and returns its (zero) data.
TEST_F(TPageCacheTest, ReadMissFetchesFromBackend)
{
    CreateCache();

    EXPECT_EQ(Backend_->GetReadCallCount(), 0);
    auto result = Read(0, PageSize);
    EXPECT_EQ(Backend_->GetReadCallCount(), 1);
    // Backend is zero-initialized.
    EXPECT_EQ(std::ssize(result), PageSize);
    for (auto byte : result) {
        EXPECT_EQ(byte, 0);
    }
}

// A second read of the same page is served from the cache (no backend call).
TEST_F(TPageCacheTest, ReadHitDoesNotHitBackend)
{
    CreateCache();

    Read(0, PageSize);
    EXPECT_EQ(Backend_->GetReadCallCount(), 1);
    Read(0, PageSize);
    EXPECT_EQ(Backend_->GetReadCallCount(), 1);
}

// Overwriting a cached page returns the latest data on read.
TEST_F(TPageCacheTest, OverwriteReturnsLatestData)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x11));
    Write(0, MakeData(PageSize, 0x22));

    auto result = Read(0, PageSize);
    EXPECT_TRUE(RefsEqual(result, MakeData(PageSize, 0x22)));
}

// A partial (sub-page) write to an uncached page triggers read-before-write:
// the rest of the page is fetched from the backend and preserved.
TEST_F(TPageCacheTest, PartialWriteReadsBeforeWrite)
{
    CreateCache();

    // Pre-populate the backend page with a known pattern.
    WaitForSync(Backend_->Write(0, MakeData(PageSize, 0x77), {}));

    // Write only the first 1 KB of the page through the cache.
    Write(0, MakeData(1_KB, 0xAB));

    // Read the full page back: first 1 KB is our write, last 3 KB is backend data.
    auto result = Read(0, PageSize);
    EXPECT_EQ(std::ssize(result), PageSize);
    EXPECT_TRUE(RefsEqual(result.Slice(0, 1_KB), MakeData(1_KB, 0xAB)));
    EXPECT_TRUE(RefsEqual(result.Slice(1_KB, 4_KB), MakeData(3_KB, 0x77)));
}

// Flush writes all dirty pages to the backend and issues a backend Flush.
TEST_F(TPageCacheTest, FlushWritesBackAndFlushesBackend)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x01));
    Write(PageSize, MakeData(PageSize, 0x02));

    EXPECT_EQ(Backend_->GetWriteBatchCallCount(), 0);
    EXPECT_EQ(Backend_->GetFlushCallCount(), 0);

    Flush();

    EXPECT_GT(Backend_->GetWriteBatchCallCount(), 0);
    EXPECT_EQ(Backend_->GetFlushCallCount(), 1);

    // Backend storage now reflects the written pages.
    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(0, PageSize), MakeData(PageSize, 0x01)));
    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(PageSize, PageSize), MakeData(PageSize, 0x02)));
}

// After Flush, dirty pages become clean and can be evicted; a subsequent read
// of an evicted page is served from the backend (which now has the data).
TEST_F(TPageCacheTest, FlushMakesPagesClean)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x42));
    Flush();

    // The backend now has the data; even if the page were evicted, a read
    // would return the correct bytes. Verify the backend was written.
    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(0, PageSize), MakeData(PageSize, 0x42)));
}

// FUA write (Flush=true) writes directly to the backend with Flush=true and
// does not require a separate Flush() call.
TEST_F(TPageCacheTest, FuaWriteGoesDirectlyToBackend)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x55), /*flush*/ true);

    EXPECT_EQ(Backend_->GetFuaWriteCount(), 1);
    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(0, PageSize), MakeData(PageSize, 0x55)));
}

// FUA write still updates the cache so a subsequent read is a cache hit.
TEST_F(TPageCacheTest, FuaWriteUpdatesCache)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x55), /*flush*/ true);

    EXPECT_EQ(Backend_->GetReadCallCount(), 0);
    auto result = Read(0, PageSize);
    EXPECT_EQ(Backend_->GetReadCallCount(), 0);
    EXPECT_TRUE(RefsEqual(result, MakeData(PageSize, 0x55)));
}

// ReadBatch is not supported and returns an error.
TEST_F(TPageCacheTest, ReadBatchReturnsError)
{
    CreateCache();

    auto future = Cache_->ReadBatch({{0, PageSize}}, {});
    auto result = WaitFor(std::move(future));
    EXPECT_FALSE(result.IsOK());
}

// WriteBatch is not supported and returns an error.
TEST_F(TPageCacheTest, WriteBatchReturnsError)
{
    CreateCache();

    auto future = Cache_->WriteBatch({{0, MakeData(PageSize, 0x01)}}, {});
    auto result = WaitFor(std::move(future));
    EXPECT_FALSE(result.IsOK());
}

// Writing enough dirty data to reach the soft limit triggers a background
// writeback that eventually writes some pages to the backend.
TEST_F(TPageCacheTest, SoftLimitTriggersBackgroundWriteback)
{
    CreateCache();

    // Soft limit = 4 pages. Write 5 full pages (different page indices) to
    // exceed the soft limit.
    for (int i = 0; i < 5; ++i) {
        Write(i * PageSize, MakeData(PageSize, static_cast<ui8>(0x10 + i)));
    }

    // A background writeback should have been triggered. Wait for the backend
    // to receive at least one WriteBatch.
    ASSERT_TRUE(WaitUntil([&] { return Backend_->GetWriteBatchCallCount() > 0; }));

    // At least some of the written pages reached the backend.
    EXPECT_GT(Backend_->GetWrittenSubrequestCount(), 0);
}

// Writing enough dirty data to reach the hard limit blocks the write until
// dirty data drops below the hard limit (synchronous writeback).
TEST_F(TPageCacheTest, HardLimitBlocksUntilWriteback)
{
    CreateCache();

    // Hard limit = 8 pages. Write 8 full pages to reach the hard limit.
    for (int i = 0; i < 8; ++i) {
        Write(i * PageSize, MakeData(PageSize, static_cast<ui8>(0x20 + i)));
    }

    // The 8th write should have triggered a hard-limit drain. After it returns,
    // a writeback must have reached the backend.
    EXPECT_GT(Backend_->GetWriteBatchCallCount(), 0);

    // Eventually the backend should reflect at least some of the written pages.
    ASSERT_TRUE(WaitUntil([&] { return Backend_->GetWrittenSubrequestCount() > 0; }));
}

// Eviction: filling the cache beyond capacity evicts the least-recently-used
// clean page. Dirty pages are never evicted.
TEST_F(TPageCacheTest, EvictsCleanPagesBeyondCapacity)
{
    CreateCache();

    // Capacity = 16 pages. Read 16 distinct pages (they become clean & cached).
    for (int i = 0; i < 16; ++i) {
        Read(i * PageSize, PageSize);
    }
    EXPECT_EQ(Backend_->GetReadCallCount(), 16);

    // Read a 17th page — the cache must evict the LRU clean page (page 0).
    Read(16 * PageSize, PageSize);

    // Re-read page 0: it was evicted, so it must go to the backend again.
    Read(0, PageSize);
    EXPECT_GT(Backend_->GetReadCallCount(), 17);
}

// A dirty page is not evicted even when the cache is full; it stays until
// written back.
TEST_F(TPageCacheTest, DirtyPageNotEvicted)
{
    CreateCache();

    // Write (dirty) page 0.
    Write(0, MakeData(PageSize, 0x99));

    // Fill the rest of the cache with clean pages (reads).
    for (int i = 1; i < 16; ++i) {
        Read(i * PageSize, PageSize);
    }

    // Read a 17th page — eviction must come from clean pages, not page 0.
    Read(16 * PageSize, PageSize);

    // Page 0 is still cached (dirty) and reads back our written data.
    auto result = Read(0, PageSize);
    EXPECT_TRUE(RefsEqual(result, MakeData(PageSize, 0x99)));
}

// A zero-length read returns an empty buffer without touching the backend.
TEST_F(TPageCacheTest, ZeroLengthReadReturnsEmpty)
{
    CreateCache();

    auto result = Read(0, 0);
    EXPECT_EQ(std::ssize(result), 0);
    EXPECT_EQ(Backend_->GetReadCallCount(), 0);
}

// A zero-length write is a no-op that succeeds.
TEST_F(TPageCacheTest, ZeroLengthWriteSucceeds)
{
    CreateCache();

    Write(0, TSharedRef::MakeEmpty());
    EXPECT_EQ(Backend_->GetWriteCallCount(), 0);
}

// Unaligned (non-page-multiple) reads spanning multiple pages work correctly.
TEST_F(TPageCacheTest, UnalignedReadSpansPages)
{
    CreateCache();

    // Pre-fill backend with a pattern.
    WaitForSync(Backend_->Write(0, MakeData(2 * PageSize, 0x33), {}));

    // Read an unaligned range spanning two pages: [1 KB, 5 KB).
    auto result = Read(1_KB, 4_KB);
    EXPECT_EQ(std::ssize(result), 4_KB);
    // Bytes 1 KB..4 KB of page 0 are 0x33, bytes 0..1 KB of page 1 are 0x33.
    for (auto byte : result) {
        EXPECT_EQ(byte, 0x33);
    }
}

// Unaligned write spanning two pages triggers read-before-write for both
// boundary pages and overlays correctly.
TEST_F(TPageCacheTest, UnalignedWriteSpansPages)
{
    CreateCache();

    // Pre-fill backend.
    WaitForSync(Backend_->Write(0, MakeData(2 * PageSize, 0x44), {}));

    // Write [1 KB, 5 KB) with a different pattern through the cache.
    Write(1_KB, MakeData(4_KB, 0x88));

    // Read back the full two pages.
    auto result = Read(0, 2 * PageSize);
    // [0, 1 KB): backend 0x44
    EXPECT_TRUE(RefsEqual(result.Slice(0, 1_KB), MakeData(1_KB, 0x44)));
    // [1 KB, 5 KB): our write 0x88
    EXPECT_TRUE(RefsEqual(result.Slice(1_KB, 5_KB), MakeData(4_KB, 0x88)));
    // [5 KB, 8 KB): backend 0x44
    EXPECT_TRUE(RefsEqual(result.Slice(5_KB, 8_KB), MakeData(3_KB, 0x44)));
}

// After Finalize, all dirty pages are written back and the backend is flushed.
TEST_F(TPageCacheTest, FinalizeFlushesDirtyData)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x71));
    Write(PageSize, MakeData(PageSize, 0x72));

    // Finalize is called in TearDown; here we just verify the effect afterwards
    // by checking the backend before destruction. We call Finalize explicitly.
    WaitForSync(Cache_->Finalize());

    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(0, PageSize), MakeData(PageSize, 0x71)));
    EXPECT_TRUE(RefsEqual(Backend_->ReadStorage(PageSize, PageSize), MakeData(PageSize, 0x72)));
    EXPECT_GE(Backend_->GetFlushCallCount(), 1);

    // Prevent double-finalize in TearDown.
    Cache_.Reset();
}

// A backend write failure during writeback propagates through Flush.
TEST_F(TPageCacheTest, BackendWriteFailurePropagatesThroughFlush)
{
    CreateCache();

    Write(0, MakeData(PageSize, 0x01));
    Backend_->SetFailing(true);

    auto result = WaitFor(Cache_->Flush({}));
    EXPECT_FALSE(result.IsOK());

    // Restore the backend so TearDown's Finalize can succeed.
    Backend_->SetFailing(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NChunk
