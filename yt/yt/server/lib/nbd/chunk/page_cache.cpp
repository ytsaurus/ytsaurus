#include "page_cache.h"

#include "config.h"

#include <yt/yt/server/lib/nbd/profiler.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <deque>
#include <limits>

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
    , SerializedInvoker_(CreateSerializedInvoker(Invoker_))
    , PageSize_(Config_->PageSize)
    , MaxPages_(Config_->Capacity / PageSize_)
    , MaxDirtyDataPerWriteback_(Config_->MaxDirtyDataPerWriteback)
    , MaxDirtyPagesPerWriteback_(Config_->MaxDirtyDataPerWriteback / PageSize_)
    , MaxDirtyDataPerWrite_(Config_->MaxDirtyDataPerWrite)
    , MaxInflightWriteRequests_(Config_->MaxInflightWriteRequests)
    , Logger(logger.WithTag("CacheSize: %v, PageSize: %v, MaxPages: %v", Config_->Capacity, PageSize_, MaxPages_))
{
    YT_VERIFY(ChunkHandler_);
    YT_VERIFY(Invoker_);
    YT_VERIFY(MaxDirtyPagesPerWriteback_ > 0);

    Pages_.reserve(MaxPages_);

    YT_LOG_INFO("Created page cache (WritebackPeriod: %v)",
        Config_->WritebackPeriod);
}

TPageCache::~TPageCache()
{
    i64 dirtyPages = 0;
    i64 cachedPages = 0;

    {
        auto guard = Guard(Lock_);
        cachedPages = std::ssize(Pages_);
        dirtyPages = std::ssize(DirtyPagesList_);
    }

    YT_LOG_INFO("Destroying page cache (DirtyPages: %v, CachedPages: %v)",
        dirtyPages,
        cachedPages);
}

TFuture<void> TPageCache::Initialize()
{
    YT_LOG_DEBUG("Initializing page cache");

    // Chain on ChunkHandler_->Initialize() so we never block the Invoker_ thread
    // with WaitFor while SerializedInvoker_ (which runs on the same Invoker_) is idle.
    return ChunkHandler_->Initialize()
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            Start();
        }).AsyncVia(Invoker_));
}

TFuture<void> TPageCache::Finalize()
{
    YT_LOG_DEBUG("Finalizing page cache");

    // Sequential async chain — each step receives the previous step's TError so all
    // errors are collected even when an earlier step fails.  Using WaitFor here would
    // block the Invoker_ thread while SerializedInvoker_ (backed by the same Invoker_)
    // tries to run — a guaranteed deadlock.

    // Stop the periodic writeback executor first so no new writeback is triggered
    // while we are doing the final writeback below.
    return Stop()
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& stopError) {
            return Flush()
                .Apply(BIND([this, this_ = MakeStrong(this), stopError] (const TError& flushError) {
                    return ChunkHandler_->Finalize()
                        .Apply(BIND([stopError, flushError] (const TError& finalizeError) {
                            // Use an explicit list instead of stopError << flushError << finalizeError.
                            // TError::operator<< adds inner errors to the left operand without changing
                            // its own OK-ness, so if stopError.IsOK() the combined result would appear OK
                            // even when flushError or finalizeError carry failures.
                            std::vector<TError> errors;
                            if (!stopError.IsOK()) {
                                errors.push_back(stopError);
                            }
                            if (!flushError.IsOK()) {
                                errors.push_back(flushError);
                            }
                            if (!finalizeError.IsOK()) {
                                errors.push_back(finalizeError);
                            }
                            if (!errors.empty()) {
                                THROW_ERROR_EXCEPTION("Page cache finalization failed")
                                    << std::move(errors);
                            }
                        }));
                }));
        }));
}

TFuture<void> TPageCache::Flush(const TFlushOptions& options)
{
    // First write back all dirty pages to the data node via WriteBatch, then ask
    // the data node to fsync them to disk via the Flush RPC.
    return ScheduleDirtyDataWriteback(/*maxDirtyDataPerWriteback*/ std::nullopt, options.Cookie, "explicit_flush")
        .Apply(BIND([this, this_ = MakeStrong(this), cookie = options.Cookie] () {
            return ChunkHandler_->Flush({.Cookie = cookie});
        }));
}

TFuture<void> TPageCache::ScheduleDirtyDataWriteback(std::optional<i64> maxDirtyDataPerWriteback, ui64 cookie, TStringBuf reason)
{
    YT_LOG_DEBUG("Scheduling dirty data writeback (MaxDirtyDataPerWriteback: %v, Cookie: %x, Reason: %v)",
        maxDirtyDataPerWriteback,
        cookie,
        reason);

    return BIND([this, this_ = MakeStrong(this), maxDirtyDataPerWriteback, cookie, reason = TString(reason)] {
        auto startTime = TInstant::Now();

        // A dirty page snapshotted for writeback: its page index, a ref to its data,
        // and the DataGeneration at snapshot time.
        struct TDirtyPage
        {
            i64 PageIndex;
            TSharedRef Data;
            ui64 DataGeneration;
        };

        // Collect dirty pages by scanning DirtyPagesList_ under the spinlock.
        // DirtyPagesList_ is iterated in LRU order (oldest dirty first), which guarantees
        // that pages that have been dirty the longest are written back first and cannot starve.
        // This is O(dirty) rather than O(total), keeping spinlock hold time minimal.
        //
        // IMPORTANT: page.Data is a TSharedMutableRef owned by the cache. A concurrent
        // foreground write can modify it at any moment after we release Lock_. We must
        // copy bytes into a fresh immutable snapshot while the lock is held so that the
        // RPC sends a stable buffer. A bare TSharedRef alias would expose the live cache
        // page to concurrent mutations, silently corrupting the written-back data.
        std::vector<TDirtyPage> dirtyPages;
        {
            auto guard = Guard(Lock_);
            for (i64 pageIndex : DirtyPagesList_) {
                auto& page = Pages_.at(pageIndex);
                auto snapshot = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                    PageSize_,
                    {.InitializeStorage = false});
                std::copy(page.Data.Begin(), page.Data.End(), snapshot.Begin());
                dirtyPages.push_back({
                    pageIndex,
                    TSharedRef(std::move(snapshot)),
                    page.DataGeneration
                });
                if (maxDirtyDataPerWriteback && std::ssize(dirtyPages) * PageSize_ >= *maxDirtyDataPerWriteback) {
                    break;
                }
            }
        }
        auto snapshotTime = TInstant::Now();

        if (dirtyPages.empty()) {
            YT_LOG_DEBUG("No dirty pages to write back");
            return;
        }

        // Sort dirty pages by page index for contiguous range merging.
        std::sort(dirtyPages.begin(), dirtyPages.end(),
            [] (const TDirtyPage& a, const TDirtyPage& b) {
                return a.PageIndex < b.PageIndex;
            });

        YT_LOG_DEBUG("Writing back dirty pages (Count: %v, MaxDirtyPagesPerWriteback: %v)",
            std::ssize(dirtyPages),
            MaxDirtyPagesPerWriteback_);

        // Build the WriteBatch writeback ranges. Each range covers up to MaxDirtyPagesPerWriteback_
        // dirty pages; adjacent (consecutive) pages within a range are merged into a single
        // contiguous subrequest, reducing the number of subrequests and the metadata
        // overhead in the data node's WriteBatch handler. A run of adjacent pages is
        // capped at MaxDirtyDataPerWrite_ bytes so one long contiguous dirty region does
        // not become a single unbounded allocation and RPC payload.
        struct TDirtyPageWritebackRange
        {
            //! [DirtyBegin, DirtyEnd) range into dirtyPages covered by this writeback range.
            i64 DirtyBegin;
            i64 DirtyEnd;
            std::vector<TWriteBatchSubrequest> Subrequests;
        };
        std::vector<TDirtyPageWritebackRange> ranges;

        for (i64 rangeDirtyBegin = 0; rangeDirtyBegin < std::ssize(dirtyPages); rangeDirtyBegin += MaxDirtyPagesPerWriteback_) {
            i64 rangeDirtyEnd = std::min(rangeDirtyBegin + MaxDirtyPagesPerWriteback_, std::ssize(dirtyPages));

            // Merge adjacent pages into contiguous ranges.
            std::vector<TWriteBatchSubrequest> subrequests;

            i64 rangeStart = rangeDirtyBegin;
            while (rangeStart < rangeDirtyEnd) {
                // Find the end of a run of consecutive page indices, capping the merged
                // extent at MaxDirtyDataPerWrite_ bytes so a merged subrequest stays bounded.
                i64 rangeEnd = rangeStart + 1;
                while (rangeEnd < rangeDirtyEnd &&
                       (rangeEnd - rangeStart + 1) * PageSize_ <= MaxDirtyDataPerWrite_ &&
                       dirtyPages[rangeEnd].PageIndex == dirtyPages[rangeEnd - 1].PageIndex + 1)
                {
                    ++rangeEnd;
                }

                i64 numPagesInRange = rangeEnd - rangeStart;
                if (numPagesInRange == 1) {
                    // Single page — no merge needed; reference page data directly.
                    const auto& page = dirtyPages[rangeStart];
                    subrequests.push_back({page.PageIndex * PageSize_, page.Data});
                } else {
                    // Multiple adjacent pages — allocate a merged buffer and copy all pages into it.
                    i64 rangeBytes = numPagesInRange * PageSize_;
                    auto merged = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                        rangeBytes,
                        {.InitializeStorage = false});
                    for (i64 i = rangeStart; i < rangeEnd; ++i) {
                        const auto& page = dirtyPages[i];
                        i64 offsetInMerged = (i - rangeStart) * PageSize_;
                        std::copy(page.Data.Begin(), page.Data.End(), merged.Begin() + offsetInMerged);
                    }
                    i64 rangeOffset = dirtyPages[rangeStart].PageIndex * PageSize_;
                    subrequests.push_back({rangeOffset, TSharedRef(std::move(merged))});
                }

                rangeStart = rangeEnd;
            }

            YT_LOG_DEBUG("WriteBatch subrequests after merging (Pages: %v, Subrequests: %v)",
                rangeDirtyEnd - rangeDirtyBegin,
                std::ssize(subrequests));

            ranges.push_back({rangeDirtyBegin, rangeDirtyEnd, std::move(subrequests)});
        }

        // Issue the WriteBatch RPCs in a sliding window of up to MaxInflightWriteRequests_
        // concurrent requests instead of strictly one-at-a-time.
        // Each inflight entry carries the corresponding range index so we can later
        // determine exactly which ranges succeeded and which failed.
        std::vector<TError> errors;
        std::vector<bool> rangeSucceeded(std::ssize(ranges), false);
        std::deque<std::pair<i64, TFuture<TWriteResponse>>> inflight; // (rangeIndex, future)
        i64 nextRange = 0;

        auto rpcStartTime = TInstant::Now();
        while (nextRange < std::ssize(ranges) || !inflight.empty()) {
            // Fill the window up to MaxInflightWriteRequests_ concurrent RPCs.
            while (nextRange < std::ssize(ranges) && std::ssize(inflight) < MaxInflightWriteRequests_) {
                inflight.emplace_back(nextRange, ChunkHandler_->WriteBatch(ranges[nextRange].Subrequests, {.Cookie = cookie}));
                ++nextRange;
            }

            // Wait for the oldest in-flight RPC to complete (FIFO), then refill the window.
            auto [rangeIndex, future] = std::move(inflight.front());
            inflight.pop_front();

            auto result = WaitFor(future);
            if (result.IsOK()) {
                rangeSucceeded[rangeIndex] = true;
            } else {
                errors.push_back(result);
            }
        }
        auto rpcFinishTime = TInstant::Now();

        // After all WriteBatch RPCs complete, update WritebackGeneration only for pages
        // belonging to ranges whose RPC succeeded. Pages from failed ranges are left dirty
        // so they will be written back by the next writeback without any data loss.
        {
            auto guard = Guard(Lock_);
            for (i64 rangeIdx = 0; rangeIdx < std::ssize(ranges); ++rangeIdx) {
                if (!rangeSucceeded[rangeIdx]) {
                    // RPC for this range failed — leave its pages dirty.
                    continue;
                }
                const auto& range = ranges[rangeIdx];
                for (i64 i = range.DirtyBegin; i < range.DirtyEnd; ++i) {
                    const auto& dirtyPage = dirtyPages[i];
                    auto it = Pages_.find(dirtyPage.PageIndex);
                    if (it == Pages_.end()) {
                        // Page was evicted — nothing to do.
                        continue;
                    }
                    auto& page = it->second;
                    if (page.DataGeneration == dirtyPage.DataGeneration) {
                        // No concurrent write happened — the writeback succeeded for this page.
                        MakePageClean(page);
                    }
                    // If page.DataGeneration > dirtyPage.DataGeneration, a concurrent write
                    // modified the page after snapshot. The page stays dirty and will be
                    // written back next time.
                }
            }
        }

        auto failedCount = std::ssize(errors);
        i64 totalSubrequests = 0;
        for (const auto& range : ranges) {
            totalSubrequests += std::ssize(range.Subrequests);
        }
        YT_LOG_INFO("Dirty writeback completed (Pages: %v, Bytes: %v, Ranges: %v, Subrequests: %v, FailedRanges: %v, Full: %v, Reason: %v, SnapshotDuration: %v, RpcDuration: %v, TotalDuration: %v)",
            std::ssize(dirtyPages),
            std::ssize(dirtyPages) * PageSize_,
            std::ssize(ranges),
            totalSubrequests,
            failedCount,
            !maxDirtyDataPerWriteback,
            reason,
            snapshotTime - startTime,
            rpcFinishTime - rpcStartTime,
            TInstant::Now() - startTime);
        if (failedCount > 0) {
            THROW_ERROR_EXCEPTION("Failed to write back some dirty pages")
                << TErrorAttribute("failed_writes", failedCount)
                << std::move(errors);
        }
    })
        .AsyncVia(SerializedInvoker_)
        .Run();
}

TFuture<TReadResponse> TPageCache::Read(i64 offset, i64 length, const TReadOptions& options)
{
    YT_VERIFY(offset >= 0);
    YT_VERIFY(length >= 0);
    YT_VERIFY(offset <= std::numeric_limits<i64>::max() - length);

    YT_LOG_DEBUG("Reading from page cache (Offset: %v, Length: %v, Cookie: %x)",
        offset,
        length,
        options.Cookie);

    if (length == 0) {
        auto result = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
            0,
            {.InitializeStorage = false});
        return MakeFuture<TReadResponse>(TReadResponse(std::move(result), /*shouldStopUsingDevice*/ false));
    }

    // Allocate result buffer.
    auto result = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
        length,
        {.InitializeStorage = false});

    const i64 firstPageIndex = offset / PageSize_;
    const i64 lastPageIndex = (offset + length - 1) / PageSize_;

    // Per-page info collected under the lock and filled in the RPC callback.
    // InPageOffset/InPageLen support unaligned requests where the first or last
    // page is only partially covered by [offset, offset+length).
    struct TPageReadInfo
    {
        i64 PageIndex;
        i64 InPageOffset;     // byte offset within the page where the request begins
        i64 InPageLen;        // number of bytes from this page that belong to the request
        i64 PageResultOffset; // byte offset in the result buffer for this page's slice
        TSharedMutableRef Data; // full page buffer filled after RPC, before second lock
    };

    // First pass under Lock_: serve cache hits synchronously, collect misses.
    std::vector<TPageReadInfo> missingPages;
    missingPages.reserve(lastPageIndex - firstPageIndex + 1);
    bool reachedDirtyDataSoftLimit = false;

    {
        auto guard = Guard(Lock_);
        for (i64 pageIndex = firstPageIndex; pageIndex <= lastPageIndex; ++pageIndex) {
            i64 pageStart = pageIndex * PageSize_;
            i64 inPageOffset = std::max(offset, pageStart) - pageStart;
            i64 inPageEnd = std::min(offset + length, pageStart + PageSize_) - pageStart;
            i64 inPageLen = inPageEnd - inPageOffset;
            i64 pageResultOffset = pageStart + inPageOffset - offset;

            auto it = Pages_.find(pageIndex);
            if (it != Pages_.end()) {
                // Cache hit — copy the relevant slice directly into the result buffer.
                auto& page = it->second;
                std::copy(
                    page.Data.Begin() + inPageOffset,
                    page.Data.Begin() + inPageOffset + inPageLen,
                    result.Begin() + pageResultOffset);
                MakePageMru(page);
            } else {
                missingPages.push_back({pageIndex, inPageOffset, inPageLen, pageResultOffset, {}});
            }
        }

        // Check dirty watermark.
        i64 dirtyBytes = std::ssize(DirtyPagesList_) * PageSize_;
        if (dirtyBytes >= Config_->DirtyDataSoftLimit) {
            reachedDirtyDataSoftLimit = true;
        }
    }

    if (reachedDirtyDataSoftLimit) {
        // Reads don't produce dirty pages, so we never block reads on watermarks.
        // Fire-and-forget a bounded writeback to keep dirty data below the soft limit.
        MaybeScheduleBackgroundWriteback(options.Cookie, "read_dirty_soft_limit");
    }

    if (missingPages.empty()) {
        // All cache hits — return immediately.
        return MakeFuture<TReadResponse>(TReadResponse(std::move(result), /*shouldStopUsingDevice*/ false));
    }

    // Issue a single RPC covering the full (page-aligned) range of all missing pages.
    const i64 rangeBegin = missingPages.front().PageIndex * PageSize_;
    const i64 rangeEnd = (missingPages.back().PageIndex + 1) * PageSize_;

    YT_LOG_DEBUG("Cache miss, issuing read RPC (Offset: %v, Length: %v, MissingPages: %v)",
        rangeBegin,
        rangeEnd - rangeBegin,
        std::ssize(missingPages));

    return ChunkHandler_->Read(rangeBegin, rangeEnd - rangeBegin, {.Cookie = options.Cookie})
        .Apply(BIND([
            this,
            this_ = MakeStrong(this),
            result,
            missingPages = std::move(missingPages),
            rangeBegin,
            rangeEnd
        ] (const TReadResponse& rpcResponse) mutable {
            YT_VERIFY(std::ssize(rpcResponse.Data) == rangeEnd - rangeBegin);

            // Allocate full-page buffers and fill them OUTSIDE the spinlock.
            for (auto& info : missingPages) {
                i64 rpcPageOffset = info.PageIndex * PageSize_ - rangeBegin;
                info.Data = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                    PageSize_,
                    {.InitializeStorage = false});
                std::copy(
                    rpcResponse.Data.Begin() + rpcPageOffset,
                    rpcResponse.Data.Begin() + rpcPageOffset + PageSize_,
                    info.Data.Begin());
            }

            // Under the spinlock: insert pages (pointer move, no alloc/copy)
            // and copy the relevant slice of each page to the result buffer.
            auto guard = Guard(Lock_);
            for (auto& info : missingPages) {
                // Re-check: another concurrent request might have already populated this page.
                auto [it, inserted] = Pages_.try_emplace(info.PageIndex);
                if (inserted) {
                    it->second.Data = std::move(info.Data);
                    // Make new page MRU.
                    CleanPagesList_.push_back(info.PageIndex);
                    it->second.LruIter = std::prev(CleanPagesList_.end());
                } else {
                    MakePageMru(it->second);
                }

                // Copy the relevant slice of the (now-cached) page to the result buffer.
                const auto& page = it->second;
                std::copy(
                    page.Data.Begin() + info.InPageOffset,
                    page.Data.Begin() + info.InPageOffset + info.InPageLen,
                    result.Begin() + info.PageResultOffset);
            }

            TryEvictExcessCleanPages();

            return TReadResponse(std::move(result), rpcResponse.ShouldStopUsingDevice);
        }));
}

TFuture<std::vector<TReadResponse>> TPageCache::ReadBatch(
    const std::vector<TReadBatchSubrequest>& /*subrequests*/,
    const TReadOptions& /*options*/)
{
    // ReadBatch bypasses the page cache entirely — dirty cached pages would not be
    // visible to the caller, violating read-your-writes consistency. Callers must
    // use Read() to go through the cache.
    return MakeFuture<std::vector<TReadResponse>>(
        TError("ReadBatch is not supported by page cache"));
}

TFuture<TWriteResponse> TPageCache::Write(i64 offset, const TSharedRef& data, const TWriteOptions& options)
{
    const i64 length = std::ssize(data);

    YT_VERIFY(offset >= 0);
    YT_VERIFY(length >= 0);
    YT_VERIFY(offset <= std::numeric_limits<i64>::max() - length);

    YT_LOG_DEBUG("Writing to page cache (Offset: %v, Length: %v, Cookie: %x, Flush: %v)",
        offset,
        length,
        options.Cookie,
        options.Flush);

    if (length == 0) {
        return MakeFuture<TWriteResponse>(TWriteResponse{});
    }

    const i64 firstPageIndex = offset / PageSize_;
    const i64 lastPageIndex = (offset + length - 1) / PageSize_;

    TWritebackRange writebackRange;
    // Shared collection of per-page generation snapshots, populated under Lock_
    // by both the main write loop and any RBW callbacks, then moved into
    // writebackRange.DataPageInfo in doFinish before the FUA RPC.
    // Using shared_ptr so RBW lambdas and doFinish share a single vector.
    auto sharedDataPageInfo = std::make_shared<std::vector<TPageSnapshot>>();
    if (options.Flush) {
        writebackRange.Offset = offset;
        writebackRange.Data = data;
        // Pre-reserve to avoid reallocations under the spinlock.
        sharedDataPageInfo->reserve(lastPageIndex - firstPageIndex + 1);
    }

    // Partial-page misses that require a read-before-write RPC to fill the page
    // before overlaying the write bytes. At most the first and last page can be partial.
    struct TPartialPageMiss
    {
        i64 PageIndex;
        i64 InPageOffset; // byte offset within the page where our write begins
        i64 InPageLen;    // number of bytes we are writing into this page
        i64 DataOffset;   // byte offset in 'data' where our write bytes begin
    };
    std::vector<TPartialPageMiss> partialPageMisses;

    bool reachedDirtyDataSoftLimit = false;
    bool reachedDirtyDataHardLimit = false;

    {
        auto guard = Guard(Lock_);
        for (i64 pageIndex = firstPageIndex; pageIndex <= lastPageIndex; ++pageIndex) {
            i64 pageStart = pageIndex * PageSize_;
            i64 inPageOffset = std::max(offset, pageStart) - pageStart;
            i64 inPageEnd = std::min(offset + length, pageStart + PageSize_) - pageStart;
            i64 inPageLen = inPageEnd - inPageOffset;
            i64 dataOffset = pageStart + inPageOffset - offset;
            bool coversEntirePage = (inPageOffset == 0 && inPageLen == PageSize_);

            auto it = Pages_.find(pageIndex);
            if (it != Pages_.end()) {
                // Cache hit — overlay only the bytes covered by this write.
                auto& page = it->second;
                std::copy(
                    data.Begin() + dataOffset,
                    data.Begin() + dataOffset + inPageLen,
                    page.Data.Begin() + inPageOffset);
                // MakePageDirtyAndMru performs exactly one splice regardless of whether
                // the page was clean or dirty, and moves it to MRU of DirtyPagesList_.
                MakePageDirtyAndMru(page, page.DataGeneration + 1);
                if (options.Flush && coversEntirePage) {
                    // Fully-covered page in FUA path: track it for writeback generation update.
                    sharedDataPageInfo->push_back({pageIndex, page.DataGeneration});
                }
            } else if (coversEntirePage) {
                // Full-page miss — allocate and fill entirely under the spinlock.
                // Pages_.find() returned end() above and we are still holding Lock_,
                // so no concurrent insertion is possible — try_emplace always succeeds.
                auto [newIt, inserted] = Pages_.try_emplace(pageIndex);
                YT_VERIFY(inserted);
                auto& page = newIt->second;
                page.Data = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                    PageSize_,
                    {.InitializeStorage = false});
                // New page starts clean — add to CleanPagesList_.
                CleanPagesList_.push_back(pageIndex);
                page.LruIter = std::prev(CleanPagesList_.end());
                std::copy(
                    data.Begin() + dataOffset,
                    data.Begin() + dataOffset + inPageLen,
                    page.Data.Begin());
                MakePageDirtyAndMru(page, page.DataGeneration + 1);
                if (options.Flush) {
                    sharedDataPageInfo->push_back({pageIndex, page.DataGeneration});
                }
            } else {
                // Partial-page miss — need to fetch the page first (read-before-write).
                // Handled below, outside the spinlock.
                partialPageMisses.push_back({pageIndex, inPageOffset, inPageLen, dataOffset});
            }
        }

        if (!options.Flush && partialPageMisses.empty()) {
            // Check dirty watermark after all pages have been written.
            i64 dirtyBytes = std::ssize(DirtyPagesList_) * PageSize_;
            if (dirtyBytes >= Config_->DirtyDataHardLimit) {
                reachedDirtyDataHardLimit = true;
            } else if (dirtyBytes >= Config_->DirtyDataSoftLimit) {
                reachedDirtyDataSoftLimit = true;
            }
        }

        TryEvictExcessCleanPages();
    }

    // Issue read-before-write RPCs for partial-page misses (at most 2: first and last page).
    // Each RPC fetches the full page from the data node, then overlays our write bytes under Lock_.
    std::vector<TFuture<void>> rbwFutures;
    rbwFutures.reserve(partialPageMisses.size());
    for (const auto& miss : partialPageMisses) {
        i64 pageStart = miss.PageIndex * PageSize_;
        auto rbwFuture = ChunkHandler_->Read(pageStart, PageSize_, {.Cookie = options.Cookie})
            .Apply(BIND([
                this,
                this_ = MakeStrong(this),
                data,
                miss,
                sharedDataPageInfo,
                flush = options.Flush
            ] (const TReadResponse& rpcResponse) {
                auto guard = Guard(Lock_);
                // Another concurrent request may have already populated this page.
                auto [it, inserted] = Pages_.try_emplace(miss.PageIndex);
                auto& page = it->second;
                if (inserted) {
                    // New page — initialize from RPC data, then overlay our bytes.
                    YT_VERIFY(std::ssize(rpcResponse.Data) == PageSize_);
                    page.Data = TSharedMutableRef::Allocate<TPageCacheBufferTag>(
                        PageSize_,
                        {.InitializeStorage = false});
                    std::copy(
                        rpcResponse.Data.Begin(),
                        rpcResponse.Data.Begin() + PageSize_,
                        page.Data.Begin());
                    // Make new page MRU.
                    CleanPagesList_.push_back(miss.PageIndex);
                    page.LruIter = std::prev(CleanPagesList_.end());
                } else {
                    // Page already existed — fall through to MakePageDirtyAndMru.
                }
                // Must be checked before MakePageDirtyAndMru advances DataGeneration.
                const bool wasClean = inserted || page.DataGeneration == page.WritebackGeneration;
                // Overlay our write bytes on top of whatever is now in the page.
                std::copy(
                    data.Begin() + miss.DataOffset,
                    data.Begin() + miss.DataOffset + miss.InPageLen,
                    page.Data.Begin() + miss.InPageOffset);
                MakePageDirtyAndMru(page, page.DataGeneration + 1);
                if (flush && wasClean) {
                    // Partial-page RBW: track for FUA writeback generation update only
                    // if the page was clean before this write. If the page was already
                    // dirty (wasClean == false), the FUA RPC only covers our partial
                    // write range, not the full pre-existing dirty data — marking it
                    // clean here would silently discard those dirty bytes. Leave it
                    // dirty so the next background writeback can write it back properly.
                    sharedDataPageInfo->push_back({miss.PageIndex, page.DataGeneration});
                }

                TryEvictExcessCleanPages();
            }));
        rbwFutures.push_back(std::move(rbwFuture));
    }

    // Helper: apply FUA/watermark logic after all cache updates (sync or async) are done.
    // Note: if there were partial-page misses, we recheck the dirty watermark here because
    // the spinlock block above skipped it (the RBW writes hadn't completed yet).
    auto doFinish = [this, this_ = MakeStrong(this), writebackRange = std::move(writebackRange),
                     reachedDirtyDataSoftLimit, reachedDirtyDataHardLimit,
                     hasPendingPartialMisses = !partialPageMisses.empty(),
                     sharedDataPageInfo = std::move(sharedDataPageInfo),
                     options] () mutable -> TFuture<TWriteResponse> {
        if (options.Flush) {
            // FUA (Force Unit Access): write the given range directly to the data node
            // via the serialized invoker and advance WritebackGeneration.
            // By the time doFinish runs (after AllSucceeded), all RBW callbacks have completed
            // and pushed their snapshots into sharedDataPageInfo, so move it all in now.
            writebackRange.DataPageInfo = std::move(*sharedDataPageInfo);
            return ScheduleRangeWriteback(std::move(writebackRange), options.Cookie);
        }

        // If there were partial-page misses, the watermark check was deferred — do it now.
        if (hasPendingPartialMisses) {
            auto guard = Guard(Lock_);
            i64 dirtyBytes = std::ssize(DirtyPagesList_) * PageSize_;
            if (dirtyBytes >= Config_->DirtyDataHardLimit) {
                reachedDirtyDataHardLimit = true;
            } else if (dirtyBytes >= Config_->DirtyDataSoftLimit) {
                reachedDirtyDataSoftLimit = true;
            }
        }

        if (reachedDirtyDataHardLimit) {
            // Hard limit: keep joining coalesced bounded writeback passes until the
            // dirty-data amount is below the hard watermark.
            return WaitForDirtyDataBelowHardLimit(options.Cookie)
                .Apply(BIND([] { return TWriteResponse{}; }));
        }

        if (reachedDirtyDataSoftLimit) {
            // Soft limit: fire-and-forget bounded writeback to reduce dirty data.
            MaybeScheduleBackgroundWriteback(options.Cookie, "write_dirty_data_soft_limit");
        }
        return MakeFuture<TWriteResponse>(TWriteResponse{});
    };

    if (rbwFutures.empty()) {
        return doFinish();
    }

    // Wait for all read-before-write RPCs to complete, then finish.
    return AllSucceeded(std::move(rbwFutures))
        .Apply(BIND([doFinish = std::move(doFinish)] () mutable -> TFuture<TWriteResponse> {
            return doFinish();
        }));
}

TFuture<TWriteResponse> TPageCache::ScheduleRangeWriteback(TWritebackRange range, ui64 cookie)
{
    return BIND([this, this_ = MakeStrong(this), range = std::move(range), cookie] () {
        YT_LOG_DEBUG("FUA writeback: writing to data node (Offset: %v, Length: %v, Cookie: %x)",
            range.Offset,
            std::ssize(range.Data),
            cookie);

        // Write directly to the data node with Flush = true (write + sync_file_range).
        WaitFor(ChunkHandler_->Write(range.Offset, range.Data, {.Flush = true, .Cookie = cookie}))
            .ThrowOnError();

        // Advance WritebackGeneration only for pages whose DataGeneration matches
        // the snapshot taken in Write(). If a concurrent write changed DataGeneration
        // after the snapshot, the page stays dirty and will be written back later.
        {
            auto guard = Guard(Lock_);
            for (const auto& snapshot : range.DataPageInfo) {
                auto it = Pages_.find(snapshot.PageIndex);
                if (it != Pages_.end() && it->second.DataGeneration == snapshot.DataGeneration) {
                    MakePageClean(it->second);
                }
            }
        }

        return TWriteResponse{};
    })
        .AsyncVia(SerializedInvoker_)
        .Run();
}

TFuture<TWriteResponse> TPageCache::WriteBatch(
    const std::vector<TWriteBatchSubrequest>& /*subrequests*/,
    const TWriteOptions& /*options*/)
{
    // WriteBatch writes to the backend directly, leaving any corresponding cached
    // pages stale and causing subsequent reads to return outdated data. Callers
    // must use Write() to go through the cache.
    return MakeFuture<TWriteResponse>(
        TError("WriteBatch is not supported by page cache"));
}

void TPageCache::Start()
{
    if (!Config_->WritebackPeriod) {
        YT_LOG_DEBUG("Periodic writeback is disabled");
        return;
    }

    PeriodicWritebackExecutor_ = New<TPeriodicExecutor>(
        Invoker_,
        BIND_NO_PROPAGATE(&TPageCache::OnPeriodicWriteback, MakeWeak(this)),
        *Config_->WritebackPeriod);

    PeriodicWritebackExecutor_->Start();
}

TFuture<void> TPageCache::Stop()
{
    if (!PeriodicWritebackExecutor_) {
        return OKFuture;
    }

    return PeriodicWritebackExecutor_->Stop();
}

void TPageCache::MakePageMru(TPage& page)
{
    // Must be called under Lock_.
    // Every cached page owns a valid LruIter (set on insertion), pointing into
    // either CleanPagesList_ or DirtyPagesList_. Splice its node to the MRU end of whichever
    // list it currently belongs to — O(1), no lookup needed.
    YT_VERIFY(page.DataGeneration >= page.WritebackGeneration);
    bool isDirty = page.DataGeneration > page.WritebackGeneration;
    auto& lru = isDirty ? DirtyPagesList_ : CleanPagesList_;
    lru.splice(lru.end(), lru, page.LruIter);
}

void TPageCache::MakePageDirtyAndMru(TPage& page, ui64 newDataGeneration)
{
    // Must be called under Lock_.
    // Set the new DataGeneration and move the page to the MRU end of DirtyPagesList_
    // in exactly one O(1) splice, regardless of whether the page was previously clean or dirty.
    YT_VERIFY(page.DataGeneration >= page.WritebackGeneration);
    YT_VERIFY(newDataGeneration >= page.WritebackGeneration);
    YT_VERIFY(newDataGeneration >= page.DataGeneration);
    const bool wasClean = page.DataGeneration == page.WritebackGeneration;
    if (wasClean) {
        // Transfer directly from current Clean position → MRU of DirtyPagesList_.
        DirtyPagesList_.splice(DirtyPagesList_.end(), CleanPagesList_, page.LruIter);
    } else {
        // Already dirty — move to MRU of DirtyPagesList_.
        DirtyPagesList_.splice(DirtyPagesList_.end(), DirtyPagesList_, page.LruIter);
    }
    page.DataGeneration = newDataGeneration;
}

void TPageCache::MakePageClean(TPage& page)
{
    // Must be called under Lock_.
    // Advance WritebackGeneration to DataGeneration, marking the page exactly clean.
    // If the page was dirty, move it from DirtyPagesList_ to CleanPagesList_.
    // Callers only invoke this after verifying DataGeneration == snapshot.DataGeneration,
    // which guarantees no concurrent write modified the page since the writeback snapshot.
    YT_VERIFY(page.DataGeneration >= page.WritebackGeneration);
    const bool wasDirty = page.DataGeneration > page.WritebackGeneration;
    if (wasDirty) {
        CleanPagesList_.splice(CleanPagesList_.end(), DirtyPagesList_, page.LruIter);
    }

    page.WritebackGeneration = page.DataGeneration;
}

i64 TPageCache::EvictCleanPages(i64 count)
{
    // Must be called under Lock_.
    i64 evicted = 0;
    for (auto it = CleanPagesList_.begin(); it != CleanPagesList_.end() && evicted < count; ) {
        i64 pageIndex = *it;
        auto pageIt = Pages_.find(pageIndex);
        YT_VERIFY(pageIt != Pages_.end());
        ++it;
        RemovePage(pageIt);
        ++evicted;
    }

    if (evicted > 0) {
        YT_LOG_DEBUG("Evicted clean pages (Count: %v)", evicted);
    } else if (count > 0) {
        YT_LOG_DEBUG("Cannot evict: no clean pages (DirtyPages: %v)", std::ssize(DirtyPagesList_));
    }

    return evicted;
}

i64 TPageCache::TryEvictExcessCleanPages()
{
    // Must be called under Lock_.
    i64 excess = std::ssize(Pages_) - MaxPages_;
    if (excess <= 0) {
        return 0;
    }
    return EvictCleanPages(excess);
}

void TPageCache::RemovePage(TPageMap::iterator it)
{
    // Must be called under Lock_.
    // Only clean pages are evictable; EvictCleanPages iterates CleanPagesList_ and calls
    // RemovePage only for clean pages. A dirty page in RemovePage indicates a bug.
    YT_VERIFY(it->second.DataGeneration == it->second.WritebackGeneration);
    CleanPagesList_.erase(it->second.LruIter);
    Pages_.erase(it);
}

TFuture<void> TPageCache::GetOrScheduleWriteback(ui64 cookie, TStringBuf reason)
{
    TPromise<void> promise;
    TFuture<void> future;
    {
        auto guard = Guard(Lock_);
        if (InflightWriteback_) {
            return *InflightWriteback_;
        }

        // Publish the future before queueing the task. This makes the check and
        // scheduling decision atomic with respect to all concurrent trigger paths.
        promise = NewPromise<void>();
        future = promise.ToFuture();
        InflightWriteback_ = future;
    }

    YT_LOG_DEBUG("Scheduling writeback (Reason: %v, MaxDirtyDataPerWriteback: %v, Cookie: %x)",
        reason,
        MaxDirtyDataPerWriteback_,
        cookie);

    ScheduleDirtyDataWriteback(MaxDirtyDataPerWriteback_, cookie, reason).Subscribe(
        BIND([this, this_ = MakeStrong(this), promise, reason = TString(reason)] (const TError& error) {
            {
                auto guard = Guard(Lock_);
                // This callback owns the only active automatic writeback. Clear the
                // slot before completing its promise, allowing continuations to start
                // a subsequent batch if dirty data remains above the hard limit.
                InflightWriteback_.reset();
            }

            promise.Set(error);
            if (!error.IsOK()) {
                YT_LOG_WARNING(error, "Writeback failed (Reason: %v)", reason);
            }
        }));

    return future;
}

TFuture<void> TPageCache::WaitForDirtyDataBelowHardLimit(ui64 cookie)
{
    {
        auto guard = Guard(Lock_);
        i64 dirtyBytes = std::ssize(DirtyPagesList_) * PageSize_;
        if (dirtyBytes < Config_->DirtyDataHardLimit) {
            return OKFuture;
        }
    }

    // If this bounded pass does not bring dirty data below the hard limit, recurse
    // through SerializedInvoker_ so the recheck never runs inline in the completion context.
    // Any writeback error propagates to the caller.
    return GetOrScheduleWriteback(cookie, "write_dirty_data_hard_limit")
        .Apply(BIND([this, this_ = MakeStrong(this), cookie] {
            return WaitForDirtyDataBelowHardLimit(cookie);
        }).AsyncVia(SerializedInvoker_));
}

void TPageCache::MaybeScheduleBackgroundWriteback(ui64 cookie, TStringBuf reason)
{
    // The future is retained in InflightWriteback_ until completion. No per-trigger
    // subscription is needed: GetOrScheduleWriteback logs any failure itself.
    YT_UNUSED_FUTURE(GetOrScheduleWriteback(cookie, reason));
}

void TPageCache::OnPeriodicWriteback()
{
    YT_LOG_DEBUG("Periodic writeback triggered");

    // Delegate to MaybeScheduleBackgroundWriteback which already owns the CAS
    // and fire-and-forget logic.  This avoids blocking the Invoker_ thread with
    // WaitFor while SerializedInvoker_ (backed by the same Invoker_) tries to run.
    MaybeScheduleBackgroundWriteback(/*cookie*/ 0, "periodic");
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TPageCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
