#include "page_cache.h"

#include "config.h"
#include <yt/yt/server/lib/nbd/profiler.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <library/cpp/yt/memory/blob.h>

namespace NYT::NNbd::NChunk {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TPageCacheBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TPageCache::TPageCache(
    TPageCacheConfigPtr config,
    IChunkHandlerPtr chunkHandler,
    IInvokerPtr invoker,
    TLogger logger)
    : Config_(std::move(config))
    , ChunkHandler_(std::move(chunkHandler))
    , Invoker_(std::move(invoker))
    , PageSize_(Config_->PageSize)
    , MaxPages_(Config_->Size / PageSize_)
    , FlushBatchSize_(Config_->FlushBatchSize)
    , Logger(logger.WithTag("CacheSize: %v, PageSize: %v, MaxPages: %v", Config_->Size, PageSize_, MaxPages_))
{
    YT_VERIFY(ChunkHandler_);
    YT_VERIFY(Invoker_);

    YT_LOG_INFO("Created page cache (FlushPeriod: %v)",
        Config_->FlushPeriod);
}

TPageCache::~TPageCache()
{
    YT_LOG_INFO("Destroying page cache (DirtyPages: %v, TotalPages: %v)",
        std::count_if(Pages_.begin(), Pages_.end(), [] (const auto& p) { return p.second.Dirty; }),
        TotalPages_);
}

TFuture<void> TPageCache::Initialize()
{
    YT_LOG_DEBUG("Initializing page cache");

    return BIND([this, this_ = MakeStrong(this)] () {
        WaitFor(ChunkHandler_->Initialize()).ThrowOnError();
        Start();
    })
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TPageCache::Finalize()
{
    YT_LOG_DEBUG("Finalizing page cache");

    return BIND([this, this_ = MakeStrong(this)] () {
        // Stop the periodic flush executor first so no new flush is triggered
        // while we are doing the final flush below.
        auto stopError = WaitFor(Stop());
        auto flushError = WaitFor(Flush());
        auto finalizeError = WaitFor(ChunkHandler_->Finalize());

        TError combinedError = stopError << flushError << finalizeError;
        if (!combinedError.IsOK()) {
            THROW_ERROR combinedError;
        }
    })
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TPageCache::Flush()
{
    return FlushBatch(std::numeric_limits<int>::max());
}

TFuture<void> TPageCache::FlushBatch(int maxPages)
{
    YT_LOG_DEBUG("Flushing page cache (MaxPages: %v)", maxPages);

    return BIND([this, this_ = MakeStrong(this), maxPages] {
        // Acquire the writer lock and HOLD it (via RAII on this scope) for the entire
        // flush — through the Write-RPCs and until they complete. While the writer lock
        // is held, no Read()/Write()/Flush() can run, so page data cannot be modified,
        // no page can be evicted, and no second flush can overlap. This makes the flush
        // race-free at the cost of blocking reads/writes for the flush duration.
        auto writerGuard = WaitFor(TAsyncLockWriterGuard::Acquire(&FlushLock_))
            .ValueOrThrow();

        // Collect dirty pages and mark them clean. Because the writer lock is held for
        // the whole flush, page.Data is stable and cannot be freed or modified
        // concurrently, so we reference it directly without copying.
        std::vector<std::pair<i64, TSharedRef>> dirtyPages;
        {
            auto guard = Guard(Lock_);
            for (auto& [pageIndex, page] : Pages_) {
                if (page.Dirty) {
                    page.Dirty = false;
                    dirtyPages.emplace_back(pageIndex, TSharedRef(page.Data));
                    if (std::ssize(dirtyPages) >= maxPages) {
                        break;
                    }
                }
            }
        }

        if (dirtyPages.empty()) {
            YT_LOG_DEBUG("No dirty pages to flush");
            return;
        }

        YT_LOG_DEBUG("Flushing dirty pages (Count: %v, BatchSize: %v)", dirtyPages.size(), FlushBatchSize_);

        // Write dirty pages to the data node in batches of FlushBatchSize_ to avoid
        // overwhelming the data node's request queue (which has a per-method limit).
        // Within each batch, pages are written in parallel; batches are sequential.
        std::vector<TError> errors;

        for (i64 batchStart = 0; batchStart < std::ssize(dirtyPages); batchStart += FlushBatchSize_) {
            i64 batchEnd = std::min(batchStart + FlushBatchSize_, std::ssize(dirtyPages));

            std::vector<TFuture<void>> writeFutures;
            writeFutures.reserve(batchEnd - batchStart);

            for (i64 i = batchStart; i < batchEnd; ++i) {
                const auto& [pageIndex, data] = dirtyPages[i];
                i64 pageOffset = pageIndex * PageSize_;
                writeFutures.push_back(
                    ChunkHandler_->Write(pageOffset, data, {})
                        .AsVoid());
            }

            auto results = WaitFor(AllSet(writeFutures))
                .ValueOrThrow();

            // On partial failure, re-mark only the pages whose writes failed as dirty so
            // the next flush retries just those. results[j] corresponds to dirtyPages[batchStart + j].
            {
                auto guard = Guard(Lock_);
                for (i64 j = 0; j < std::ssize(results); ++j) {
                    if (!results[j].IsOK()) {
                        // The writer lock is held for the whole flush, so no eviction could
                        // have run and the page must still exist.
                        auto it = Pages_.find(dirtyPages[batchStart + j].first);
                        YT_VERIFY(it != Pages_.end());
                        it->second.Dirty = true;
                        errors.push_back(results[j]);
                    }
                }
            }
        }

        if (!errors.empty()) {
            int failedCount = errors.size();
            THROW_ERROR_EXCEPTION("Failed to flush some dirty pages")
                << TErrorAttribute("failed_writes", failedCount)
                << std::move(errors);
        }
        YT_LOG_DEBUG("Flushed dirty pages (Count: %v)", dirtyPages.size());
    })
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<TReadResponse> TPageCache::Read(i64 offset, i64 length, const TReadOptions& options)
{
    YT_LOG_DEBUG("Reading from page cache (Offset: %v, Length: %v, Cookie: %x)",
        offset,
        length,
        options.Cookie);

    if (auto error = CheckHardLimit()) {
        return MakeFuture<TReadResponse>(*std::move(error));
    }

    // Acquire reader lock on FlushLock_ so that Flush() writer lock waits for all
    // in-flight Read/Write calls before taking a snapshot of dirty pages.
    return TAsyncLockReaderGuard::Acquire(&FlushLock_)
        .Apply(BIND(
            [=, this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncLockReaderGuard> readerGuard) mutable -> TFuture<TReadResponse> {
                // Allocate result buffer.
                auto resultMutable = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                    length,
                    {.InitializeStorage = false});

                i64 firstPageIndex = offset / PageSize_;
                i64 lastPageIndex = (offset + length - 1) / PageSize_;

                // Per-page info needed later in the RPC callback.
                struct TPageReadInfo {
                    i64 PageIndex;
                    i64 InPageOffset;
                    i64 InPageLen;
                    i64 PageResultOffset;
                };

                // First pass under Lock_: serve cache hits synchronously, collect misses.
                std::vector<TPageReadInfo> missingPages;
                bool needFlush = false;

                {
                    auto guard = Guard(Lock_);
                    for (i64 pageIndex = firstPageIndex; pageIndex <= lastPageIndex; ++pageIndex) {
                        i64 pageStart = pageIndex * PageSize_;
                        i64 inPageOffset = std::max(offset, pageStart) - pageStart;
                        i64 inPageEnd = std::min(offset + length, pageStart + PageSize_) - pageStart;
                        i64 inPageLen = inPageEnd - inPageOffset;
                        i64 pageResultOffset = (pageStart + inPageOffset) - offset;

                        auto it = Pages_.find(pageIndex);
                        if (it != Pages_.end()) {
                            // Cache hit — copy data directly into result buffer.
                            const auto& page = it->second;
                            std::copy(
                                page.Data.Begin() + inPageOffset,
                                page.Data.Begin() + inPageOffset + inPageLen,
                                resultMutable.Begin() + pageResultOffset);
                            TouchPage(pageIndex);
                        } else {
                            missingPages.push_back({pageIndex, inPageOffset, inPageLen, pageResultOffset});
                        }
                    }

                    // Evict enough clean pages to make room for all missing pages.
                    // A single O(N) pass through the LRU list instead of O(N * numMissing).
                    i64 numMissing = std::ssize(missingPages);
                    i64 toEvict = std::max<i64>(0, TotalPages_ + numMissing - MaxPages_);
                    if (toEvict > 0) {
                        i64 evicted = EvictPages(toEvict);
                        if (evicted < toEvict) {
                            // Could not evict enough pages (all remaining are dirty) —
                            // trigger a background flush to free up space.
                            needFlush = true;
                        }
                    }
                }

                if (needFlush) {
                    YT_LOG_DEBUG("Cache is full of dirty pages, triggering background flush");
                    TriggerBackgroundFlush("Background flush triggered by cache pressure failed");
                }

                if (missingPages.empty()) {
                    // All cache hits — return immediately.
                    // readerGuard is released here.
                    auto result = TSharedRef(std::move(resultMutable));
                    return MakeFuture<TReadResponse>(TReadResponse(std::move(result), /*shouldStopUsingDevice*/ false));
                }

                // Issue a single bulk RPC covering the full range of all missing pages.
                // This avoids N round-trips to the data node for N cache misses.
                i64 bulkStart = missingPages.front().PageIndex * PageSize_;
                i64 bulkEnd = (missingPages.back().PageIndex + 1) * PageSize_;

                YT_LOG_DEBUG("Cache miss, issuing bulk read RPC (BulkOffset: %v, BulkLength: %v, MissingPages: %v)",
                    bulkStart,
                    bulkEnd - bulkStart,
                    missingPages.size());

                return ChunkHandler_->Read(bulkStart, bulkEnd - bulkStart, {.Cookie = options.Cookie})
                    .Apply(BIND([
                        this,
                        this_ = MakeStrong(this),
                        resultMutable,
                        missingPages = std::move(missingPages),
                        bulkStart,
                        readerGuard = std::move(readerGuard)
                    ] (const TReadResponse& rpcResponse) mutable {
                        // Pre-build page buffers OUTSIDE the spinlock to avoid holding it
                        // across heap allocation and full-page memcpy (N × PageSize_ bytes).
                        // Each buffer is allocated and filled from the bulk RPC response here;
                        // the spinlock below only does cheap pointer/iterator insertions.
                        std::vector<TSharedMutableRef> prebuiltData;
                        prebuiltData.reserve(missingPages.size());
                        for (const auto& info : missingPages) {
                            i64 rpcPageOffset = info.PageIndex * PageSize_ - bulkStart;
                            auto data = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                                PageSize_,
                                {.InitializeStorage = false});
                            std::copy(
                                rpcResponse.Data.Begin() + rpcPageOffset,
                                rpcResponse.Data.Begin() + rpcPageOffset + PageSize_,
                                data.Begin());
                            prebuiltData.push_back(std::move(data));
                        }

                        // Under the spinlock: insert pages (pointer move, no alloc/copy)
                        // and copy the relevant slice of each page to the result buffer.
                        auto guard = Guard(Lock_);
                        for (i64 i = 0; i < std::ssize(missingPages); ++i) {
                            const auto& info = missingPages[i];

                            // Re-check: another concurrent request might have already populated this page.
                            // try_emplace inserts a default TPage if absent and returns the iterator in one lookup.
                            auto [it, inserted] = Pages_.try_emplace(info.PageIndex);
                            if (inserted) {
                                it->second = {.Data = std::move(prebuiltData[i]), .Dirty = false};
                                LruList_.push_back(info.PageIndex);
                                LruMap_[info.PageIndex] = std::prev(LruList_.end());
                                TotalPages_++;
                            }
                            TouchPage(info.PageIndex);

                            // Copy the relevant portion of the (now-cached) page to the result buffer.
                            const auto& cachedPage = it->second;
                            std::copy(
                                cachedPage.Data.Begin() + info.InPageOffset,
                                cachedPage.Data.Begin() + info.InPageOffset + info.InPageLen,
                                resultMutable.Begin() + info.PageResultOffset);
                        }
                        // readerGuard is released here, after all cache insertions are done.
                        auto result = TSharedRef(std::move(resultMutable));
                        // Propagate ShouldStopUsingDevice from the data node RPC response.
                        return TReadResponse(std::move(result), rpcResponse.ShouldStopUsingDevice);
                    }));
            }));
}

TFuture<TWriteResponse> TPageCache::Write(i64 offset, const TSharedRef& data, const TWriteOptions& options)
{
    YT_LOG_DEBUG("Writing to page cache (Offset: %v, Length: %v, Cookie: %x, Flush: %v)",
        offset,
        data.size(),
        options.Cookie,
        options.Flush);

    if (auto error = CheckHardLimit()) {
        return MakeFuture<TWriteResponse>(*std::move(error));
    }

    // Acquire reader lock on FlushLock_ so that Flush() writer lock waits for all
    // in-flight Read/Write calls before taking a snapshot of dirty pages.
    return TAsyncLockReaderGuard::Acquire(&FlushLock_)
        .Apply(BIND(
            [=, this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncLockReaderGuard> readerGuard) mutable -> TFuture<TWriteResponse> {
                i64 length = data.size();
                i64 firstPageIndex = offset / PageSize_;
                i64 lastPageIndex = (offset + length - 1) / PageSize_;

                // Collect read-before-write futures for partial pages.
                std::vector<TFuture<void>> readBeforeWriteFutures;

                // First pass under Lock_: serve cache hits, collect misses for bulk eviction.
                struct TPageWriteInfo {
                    i64 PageIndex;
                    i64 InPageOffset;
                    i64 InPageLen;
                    i64 DataOffset;
                };
                std::vector<TPageWriteInfo> missingPages;

                {
                    auto guard = Guard(Lock_);
                    for (i64 pageIndex = firstPageIndex; pageIndex <= lastPageIndex; ++pageIndex) {
                        i64 pageStart = pageIndex * PageSize_;
                        i64 inPageOffset = std::max(offset, pageStart) - pageStart;
                        i64 inPageEnd = std::min(offset + length, pageStart + PageSize_) - pageStart;
                        i64 inPageLen = inPageEnd - inPageOffset;
                        i64 dataOffset = (pageStart + inPageOffset) - offset;

                        auto it = Pages_.find(pageIndex);
                        if (it != Pages_.end()) {
                            // Page already in cache — just update the relevant bytes.
                            auto& page = it->second;
                            std::copy(
                                data.Begin() + dataOffset,
                                data.Begin() + dataOffset + inPageLen,
                                page.Data.Begin() + inPageOffset);
                            page.Dirty = true;
                            TouchPage(pageIndex);
                        } else {
                            missingPages.push_back({pageIndex, inPageOffset, inPageLen, dataOffset});
                        }
                    }

                    // Evict enough clean pages to make room for all missing pages.
                    // A single O(N) pass through the LRU list instead of O(N * numMissing).
                    i64 numMissing = std::ssize(missingPages);
                    i64 toEvict = std::max<i64>(0, TotalPages_ + numMissing - MaxPages_);
                    bool needFlush = false;
                    if (toEvict > 0) {
                        i64 evicted = EvictPages(toEvict);
                        if (evicted < toEvict) {
                            needFlush = true;
                        }
                    }
                    guard.Release();

                    if (needFlush) {
                        YT_LOG_DEBUG("Cache is full of dirty pages, triggering background flush");
                        TriggerBackgroundFlush("Background flush triggered by cache pressure failed");
                    }
                }

                // Second pass: process each missing page (full-page or partial-page write).
                for (const auto& info : missingPages) {
                    i64 pageIndex = info.PageIndex;
                    i64 inPageOffset = info.InPageOffset;
                    i64 inPageLen = info.InPageLen;
                    i64 dataOffset = info.DataOffset;
                    bool coversEntirePage = (inPageOffset == 0 && inPageLen == PageSize_);

                    if (coversEntirePage) {
                        // Full page write — no read-before-write needed.
                        // Allocate and fill the new page buffer OUTSIDE the spinlock.
                        auto pageData = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                            PageSize_,
                            {.InitializeStorage = false});
                        std::copy(
                            data.Begin() + dataOffset,
                            data.Begin() + dataOffset + inPageLen,
                            pageData.Begin());

                        // Under the spinlock: insert the page (pointer move, no alloc/copy).
                        auto guard = Guard(Lock_);
                        // Re-check: a concurrent request might have populated this page
                        // while the spinlock was released.
                        auto [it, inserted] = Pages_.try_emplace(pageIndex);
                        if (inserted) {
                            it->second.Data = std::move(pageData);
                            it->second.Dirty = true;
                            LruList_.push_back(pageIndex);
                            LruMap_[pageIndex] = std::prev(LruList_.end());
                            TotalPages_++;
                        } else {
                            // Another request raced and already inserted this page.
                            // Overwrite it with our data (inPageLen == PageSize_ here).
                            std::copy(
                                data.Begin() + dataOffset,
                                data.Begin() + dataOffset + inPageLen,
                                it->second.Data.Begin() + inPageOffset);
                            it->second.Dirty = true;
                        }
                        TouchPage(pageIndex);
                    } else {
                        // Partial page write — need read-before-write.
                        i64 pageStart = pageIndex * PageSize_;
                        auto readFuture = ChunkHandler_->Read(pageStart, PageSize_, {.Cookie = options.Cookie})
                            .Apply(BIND([
                                this,
                                this_ = MakeStrong(this),
                                pageIndex,
                                data,
                                inPageOffset,
                                inPageLen,
                                dataOffset
                            ] (const TReadResponse& rpcResponse) mutable {
                                auto guard = Guard(Lock_);
                                // Re-check: another request might have populated this page.
                                auto [it, inserted] = Pages_.try_emplace(pageIndex);
                                if (inserted) {
                                    it->second.Data = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                                        PageSize_,
                                        {.InitializeStorage = false});
                                    std::copy(
                                        rpcResponse.Data.Begin(),
                                        rpcResponse.Data.Begin() + PageSize_,
                                        it->second.Data.Begin());
                                    it->second.Dirty = false;
                                    LruList_.push_back(pageIndex);
                                    LruMap_[pageIndex] = std::prev(LruList_.end());
                                    TotalPages_++;
                                }
                                std::copy(
                                    data.Begin() + dataOffset,
                                    data.Begin() + dataOffset + inPageLen,
                                    it->second.Data.Begin() + inPageOffset);
                                it->second.Dirty = true;
                                TouchPage(pageIndex);
                            }));

                        readBeforeWriteFutures.push_back(std::move(readFuture));
                    }
                }

                // If FUA (Force Unit Access) is requested, flush dirty pages after write.
                if (options.Flush) {
                    return AllSucceeded(readBeforeWriteFutures)
                        .Apply(BIND([this, this_ = MakeStrong(this), readerGuard = std::move(readerGuard)] () mutable -> TFuture<TWriteResponse> {
                            // Release FlushLock_ reader lock before calling Flush():
                            // Flush() acquires the writer lock and waits for all readers.
                            // Keeping the reader lock here would cause a deadlock.
                            readerGuard.Reset();
                            return Flush().Apply(BIND([] {
                                return TWriteResponse{};
                            }));
                        })
                        .AsyncVia(Invoker_));
                }

                // Wait for read-before-write RPCs to complete, then return.
                // Keep readerGuard alive until all page.Data modifications are done.
                // AllSucceeded on an empty vector returns a ready future, so this
                // also handles the all-cache-hits case (no missing pages).
                return AllSucceeded(readBeforeWriteFutures)
                    .Apply(BIND([readerGuard = std::move(readerGuard)] () {
                        // readerGuard is released here, after all page.Data writes complete.
                        return TWriteResponse{};
                    }));
            }));
}

void TPageCache::Start()
{
    if (!Config_->FlushPeriod) {
        YT_LOG_DEBUG("Periodic flush is disabled");
        return;
    }

    FlushExecutor_ = New<TPeriodicExecutor>(
        Invoker_,
        BIND_NO_PROPAGATE(&TPageCache::OnPeriodicFlush, MakeWeak(this)),
        *Config_->FlushPeriod);

    FlushExecutor_->Start();
}

TFuture<void> TPageCache::Stop()
{
    if (!FlushExecutor_) {
        return OKFuture;
    }

    return FlushExecutor_->Stop();
}

void TPageCache::TouchPage(i64 pageIndex)
{
    // Must be called under Lock_.
    auto lruIt = LruMap_.find(pageIndex);
    if (lruIt != LruMap_.end()) {
        LruList_.splice(LruList_.end(), LruList_, lruIt->second);
    }
}

i64 TPageCache::EvictPages(i64 count)
{
    // Must be called under Lock_.
    // Single pass through the LRU list: evict up to |count| clean (non-dirty)
    // pages. O(N) regardless of count.
    i64 evicted = 0;
    for (auto it = LruList_.begin(); it != LruList_.end() && evicted < count; ) {
        i64 pageIndex = *it;
        auto pageIt = Pages_.find(pageIndex);
        // Skip dirty pages (data not yet flushed). A flush holds the writer lock
        // for its whole duration, so eviction never races with an in-flight write.
        if (pageIt != Pages_.end() && !pageIt->second.Dirty) {
            // RemovePage erases the LRU iterator, so advance before removing.
            ++it;
            RemovePage(pageIndex);
            YT_LOG_DEBUG("Evicted clean page (PageIndex: %v)", pageIndex);
            ++evicted;
        } else {
            ++it;
        }
    }

    if (evicted == 0) {
        // All pages are dirty — cannot evict without flushing.
        // The periodic flush will eventually clean dirty pages, allowing
        // future evictions to succeed. The cache may temporarily exceed its
        // size limit, but this is safe.
        YT_LOG_DEBUG("Cannot evict: all pages are dirty");
    }

    return evicted;
}

void TPageCache::RemovePage(i64 pageIndex)
{
    // Must be called under Lock_.
    Pages_.erase(pageIndex);
    auto lruIt = LruMap_.find(pageIndex);
    if (lruIt != LruMap_.end()) {
        LruList_.erase(lruIt->second);
        LruMap_.erase(pageIndex);
    }
    TotalPages_--;
}

std::optional<TError> TPageCache::CheckHardLimit()
{
    // If the cache has grown beyond 2 × MaxPages_ (all pages are dirty and
    // eviction is impossible), log a warning, trigger an emergency flush, and
    // return an error so the caller can fail fast and retry after the cache drains.
    auto guard = Guard(Lock_);
    if (TotalPages_ <= 2 * MaxPages_) {
        return std::nullopt;
    }
    auto totalPages = TotalPages_;
    guard.Release();
    YT_LOG_WARNING("Page cache exceeded hard size limit, triggering emergency flush "
        "(TotalPages: %v, MaxPages: %v)",
        totalPages,
        MaxPages_);
    TriggerBackgroundFlush("Emergency flush triggered by hard limit failed");
    return TError("Page cache exceeded hard size limit; retry after flush")
        << TErrorAttribute("total_pages", totalPages)
        << TErrorAttribute("max_pages", MaxPages_);
}

void TPageCache::TriggerBackgroundFlush(TStringBuf failureLogMessage)
{
    // Use FlushBatch instead of the full Flush() to limit the time the writer lock is held.
    // Full Flush() can block all reads/writes for many seconds when the cache is full;
    // FlushBatch(FlushBatchSize_) only holds the lock for one small batch (~10 ms).
    FlushBatch(FlushBatchSize_).Subscribe(BIND([Logger = Logger, failureLogMessage = TString(failureLogMessage)] (const TError& error) {
        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "%v", failureLogMessage);
        }
    }));
}

void TPageCache::OnPeriodicFlush()
{
    YT_LOG_DEBUG("Periodic flush triggered");

    // Only flush a single batch of dirty pages per periodic tick to avoid holding
    // the writer lock for too long and blocking reads/writes.
    auto future = FlushBatch(FlushBatchSize_);
    auto result = WaitFor(future);
    if (!result.IsOK()) {
        YT_LOG_ERROR(result, "Periodic flush failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TPageCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
