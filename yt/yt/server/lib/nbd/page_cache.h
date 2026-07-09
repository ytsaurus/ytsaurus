#pragma once

#include "public.h"

#include "block_device.h"
#include "chunk_handler.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/logging/logger.h>

#include <list>
#include <unordered_map>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! Page cache for write-back caching of NBD chunk block device.
//!
//! Pages are always Config->PageSize bytes and fully populated.
//! Write operations store data in the cache and mark pages as dirty;
//! dirty pages are flushed to data node asynchronously or on demand.
//! Read operations check the cache first and fall back to data node
//! on cache miss.
//!
//! Eviction policy: LRU. Only clean pages can be evicted; dirty pages
//! cannot be evicted until they are flushed. When the cache is full of
//! dirty pages, a background flush is triggered to free up space.
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

    //! Flush dirty pages to data node.
    TFuture<void> Flush() override;

    //! Read data from the cache or data node.
    //! On cache miss, reads the full page from data node and caches it.
    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) override;

    //! Write data to the cache. Marks affected pages as dirty.
    //! Returns immediately (write-back); dirty pages are flushed later.
    //! For partial-page writes (rare unaligned requests), reads necessary pages
    //! from data node first (read-before-write).
    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override;

private:
    struct TPage
    {
        TSharedMutableRef Data;
        //! Page needs to be flushed.
        bool Dirty = false;
    };

    using TPageMap = std::unordered_map<i64, TPage>;

    // LRU list: front = least recently used, back = most recently used.
    using TLruList = std::list<i64>;
    using TLruIter = TLruList::iterator;

    const TPageCacheConfigPtr Config_;
    const IChunkHandlerPtr ChunkHandler_;
    const IInvokerPtr Invoker_;
    const i64 PageSize_;
    const i64 MaxPages_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TPageMap Pages_;
    TLruList LruList_;
    std::unordered_map<i64, TLruIter> LruMap_;
    i64 TotalPages_ = 0;

    NConcurrency::TAsyncReaderWriterLock FlushLock_;

    NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    //! Start periodic background flush of dirty pages.
    void Start();

    //! Stop periodic background flush of dirty pages.
    TFuture<void> Stop();

    //! Move page to the most-recently-used end of the LRU list.
    void TouchPage(i64 pageIndex);

    //! Evict up to |count| clean pages from the cache in a single pass.
    //! Returns the number of pages actually evicted.
    //! Single O(N) pass through the LRU list regardless of count.
    i64 EvictPages(i64 count);

    //! Remove a page from the cache (both map and LRU structures).
    void RemovePage(i64 pageIndex);

    //! Trigger a background flush and log any resulting error.
    //! Fire-and-forget: does not block the caller.
    void TriggerBackgroundFlush(TStringBuf failureLogMessage);

    //! Check whether the cache has exceeded its hard size limit (2 × MaxPages_).
    //! If so, logs a warning, triggers an emergency flush, and returns the error
    //! to return to the caller. Returns std::nullopt if within the limit.
    std::optional<TError> CheckHardLimit();

    //! Periodic callback that flushes dirty pages.
    void OnPeriodicFlush();
};

DECLARE_REFCOUNTED_TYPE(TPageCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
