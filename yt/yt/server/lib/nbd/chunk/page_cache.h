#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/block_device.h>
#include "chunk_handler.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/logging/logger.h>

#include <atomic>
#include <list>
#include <optional>
#include <unordered_map>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

//! Page cache for write-back caching of NBD chunk block device.
//!
//! Pages are always Config->PageSize bytes and fully populated.
//! Write operations store data in the cache and increment the page's DataGeneration;
//! dirty pages (DataGeneration > WritebackGeneration) are written back to data node
//! asynchronously or on demand via a serialized invoker.
//! Read operations check the cache first and fall back to data node on cache miss.
//!
//! Eviction policy: LRU. Only clean pages (DataGeneration == WritebackGeneration)
//! can be evicted; dirty pages cannot be evicted until they are written back.
class TPageCache
    : public IChunkHandler
{
public:
    TPageCache(
        TPageCacheConfigPtr config,
        IChunkHandlerPtr chunkHandler,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    ~TPageCache();

    TFuture<void> Initialize() override;

    TFuture<void> Finalize() override;

    //! Write back all dirty pages, then issue a data-node Flush RPC.
    TFuture<void> Flush(const TFlushOptions& options = {}) override;

    //! Read data from the cache or data node.
    //! On cache miss, reads from data node and caches it.
    //! Supports unaligned |offset| and |length|; page boundaries are handled internally.
    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) override;

    //! Not supported — always returns an error.
    //! ReadBatch bypasses the cache, violating read-your-writes consistency.
    //! Use Read() instead.
    TFuture<std::vector<TReadResponse>> ReadBatch(
        const std::vector<TReadBatchSubrequest>& subrequests,
        const TReadOptions& options) override;

    //! Write data to the cache. Increments DataGeneration for affected pages.
    //! Returns immediately (write-back); dirty pages are written later.
    //! Supports unaligned |offset| and |std::ssize(data)|; partial boundary pages are
    //! fetched from data node (read-before-write) if not already cached.
    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override;

    //! Not supported — always returns an error.
    //! WriteBatch writes to the backend directly, leaving cached pages stale.
    //! Use Write() instead.
    TFuture<TWriteResponse> WriteBatch(
        const std::vector<TWriteBatchSubrequest>& subrequests,
        const TWriteOptions& options) override;

private:
    // LRU list type: front = least recently used, back = most recently used.
    using TLruList = std::list<i64>;
    using TLruIter = TLruList::iterator;

    struct TPage
    {
        TSharedMutableRef Data;
        //! Incremented on every write to this page.
        ui64 DataGeneration = 0;
        //! Generation at the time of the last completed writeback.
        //! DataGeneration == WritebackGeneration means the page is clean.
        //! DataGeneration > WritebackGeneration means the page is dirty.
        ui64 WritebackGeneration = 0;
        //! Position of this page in LRU list (either clean or dirty).
        TLruIter LruIter;
    };

    using TPageMap = std::unordered_map<i64, TPage>;

    const TPageCacheConfigPtr Config_;
    const IChunkHandlerPtr ChunkHandler_;
    const IInvokerPtr Invoker_;
    //! Serialized invoker for dirty-data writeback and FUA write tasks. Preserves
    //! order so these writes do not overlap and dirty-page snapshots are consistent.
    const IInvokerPtr SerializedInvoker_;
    const i64 PageSize_;
    //! Maximum number of pages in the cache.
    const i64 MaxPages_;

    //! Maximum bytes of dirty data to write back in a single writeback pass.
    const i64 MaxDirtyDataPerWriteback_;
    //! Number of dirty pages a single writeback processes (MaxDirtyDataPerWriteback / PageSize).
    const i64 MaxDirtyPagesPerWriteback_;
    //! Upper bound in bytes on the size of a single merged WriteBatch subrequest.
    const i64 MaxDirtyDataPerWrite_;
    //! Maximum number of WriteBatch RPCs in flight concurrently during writeback.
    const i64 MaxInflightWriteRequests_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TPageMap Pages_;
    //! LRU list of clean pages only (DataGeneration == WritebackGeneration).
    //! Only pages in this list are evictable, so EvictCleanPages walks this list
    //! without skipping dirty pages.
    TLruList CleanPagesList_;
    //! LRU list of dirty pages only (DataGeneration > WritebackGeneration).
    //! The number of dirty pages is std::ssize(DirtyPagesList_).
    TLruList DirtyPagesList_;

    NConcurrency::TPeriodicExecutorPtr PeriodicWritebackExecutor_;

    //! Shared future for an automatic (soft-limit, hard-limit, or periodic) writeback.
    //! Protected by Lock_. Concurrent triggers join this future instead of queuing
    //! duplicate work on SerializedInvoker_. Explicit Flush() is intentionally separate.
    std::optional<TFuture<void>> InflightWriteback_;

    //! Start periodic background writeback of dirty pages.
    void Start();

    //! Stop periodic background writeback of dirty pages.
    TFuture<void> Stop();

    //! Write dirty data back to data node. If |maxDirtyDataPerWriteback| is set, writes
    //! back up to that many dirty bytes (a partial writeback); if std::nullopt, writes
    //! back all dirty bytes (a full writeback). |reason| is a short diagnostic token
    //! recorded in log messages (e.g. "periodic", "explicit_flush",
    //! "write_dirty_data_hard_limit", "read_dirty_soft_limit").
    TFuture<void> ScheduleDirtyDataWriteback(std::optional<i64> maxDirtyDataPerWriteback, ui64 cookie = 0, TStringBuf reason = "unknown");

    //! Move page to the most-recently-used end of its current LRU list
    //! (CleanPagesList_ or DirtyPagesList_). O(1) splice, no lookup.
    //! Must be called under Lock_.
    void MakePageMru(TPage& page);

    //! Set the new DataGeneration and move the page to the MRU end of DirtyPagesList_
    //! in a single O(1) splice, regardless of whether the page was previously clean or dirty.
    //! Must be called under Lock_.
    void MakePageDirtyAndMru(TPage& page, ui64 newDataGeneration);

    //! Advance WritebackGeneration to DataGeneration (marking the page exactly clean)
    //! and move it from DirtyPagesList_ to CleanPagesList_ if it was dirty.
    //! Must be called under Lock_.
    void MakePageClean(TPage& page);

    //! Evict up to |count| clean pages from the cache in a single pass.
    //! Returns the number of pages actually evicted.
    //! Must be called under Lock_.
    i64 EvictCleanPages(i64 count);

    //! If the cache has grown beyond MaxPages_, evict excess clean pages.
    //! Returns the number of pages actually evicted.
    //! Must be called under Lock_.
    i64 TryEvictExcessCleanPages();

    //! Remove a page from the cache (both map and LRU structures).
    //! Must be called under Lock_.
    void RemovePage(TPageMap::iterator it);

    //! Snapshot of a page's state at the time of a FUA write.
    //! Used to advance WritebackGeneration only if no concurrent write occurred.
    struct TPageSnapshot {
        i64 PageIndex;
        ui64 DataGeneration;
    };

    //! A contiguous byte range written via FUA, together with per-page generation snapshots.
    struct TWritebackRange {
        i64 Offset;
        TSharedRef Data;
        std::vector<TPageSnapshot> DataPageInfo;
    };

    //! Write the given range directly to the data node with Flush = true
    //! (write + sync_file_range) and advance WritebackGeneration for pages
    //! whose DataGeneration matches the snapshot taken in TWritebackRange::DataPageInfo.
    //! If a concurrent write changed DataGeneration after the snapshot, the page
    //! stays dirty and will be written back later.
    //! Runs on the serialized invoker so it does not overlap with writeback.
    TFuture<TWriteResponse> ScheduleRangeWriteback(TWritebackRange range, ui64 cookie);

    //! Return the active automatic writeback or atomically schedule a new bounded one.
    //! The future is published under Lock_ before scheduling work, so concurrent soft-limit,
    //! hard-limit, and periodic triggers always coalesce. |reason| is recorded in logs.
    TFuture<void> GetOrScheduleWriteback(ui64 cookie, TStringBuf reason);

    //! Wait for coalesced bounded writeback passes until dirty data is below the hard limit.
    //! Each completed pass is followed by a locked watermark recheck so concurrent writers
    //! cannot let dirty data remain at or above DirtyDataHardLimit.
    TFuture<void> WaitForDirtyDataBelowHardLimit(ui64 cookie);

    //! Schedule a bounded automatic writeback if none is active; otherwise return immediately.
    void MaybeScheduleBackgroundWriteback(ui64 cookie, TStringBuf reason);

    //! Periodic callback that writes dirty pages back to data node.
    void OnPeriodicWriteback();
};

DECLARE_REFCOUNTED_TYPE(TPageCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
