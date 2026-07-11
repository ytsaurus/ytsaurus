#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"

#include <yt/yt/server/lib/nbd/block_device_detail.h>
#include "page_cache.h"
#include <yt/yt/server/lib/nbd/profiler.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd::NChunk {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NRpc;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockDevice
    : public TBlockDeviceBase
{
public:
    TChunkBlockDevice(
        std::string exportId,
        TChunkBlockDeviceConfigPtr config,
        IThroughputThrottlerPtr readThrottler,
        IThroughputThrottlerPtr writeThrottler,
        IInvokerPtr invoker,
        IChannelPtr channel,
        TSessionId sessionId,
        TLogger logger)
        : ExportId_(std::move(exportId))
        , Config_(std::move(config))
        , ReadThrottler_(std::move(readThrottler))
        , WriteThrottler_(std::move(writeThrottler))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , ChunkHandler_(CreateRequestHandler(
            this,
            Config_,
            Invoker_,
            std::move(channel),
            sessionId,
            Logger))
    {
        TNbdProfilerCounters::Get()->GetCounter(
            TNbdProfilerCounters::MakeTagSet(SensorTag_), "/device/created")
                .Increment(1);

        YT_LOG_INFO("Created chunk block device (Size: %v, Filesystem: %v)",
            Config_->Size,
            Config_->FsType);
    }

    ~TChunkBlockDevice()
    {
        TNbdProfilerCounters::Get()->GetCounter(
            TNbdProfilerCounters::MakeTagSet(SensorTag_), "/device/removed")
                .Increment(1);

        YT_LOG_INFO("Destructing chunk block device (Size: %v, Filesystem: %v)",
            Config_->Size,
            Config_->FsType);
    }

    i64 GetTotalSize() const override
    {
        return Config_->Size;
    }

    i64 GetBlockSize() const override
    {
        // Byte-addressable backend; advertise the conventional sector size.
        return 512;
    }

    bool IsReadOnly() const override
    {
        return false;
    }

    std::string GetDescription() const override
    {
        return Format("Chunk{Size: %v}", GetTotalSize());
    }

    std::string GetProfileSensorTag() const override
    {
        return SensorTag_;
    }

    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) override
    {
        YT_LOG_DEBUG("Started reading from chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            length,
            options.Cookie);

        if (length == 0) {
            YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);

            return MakeFuture<TReadResponse>({});
        }

        // Acquire reader lock to prevent concurrent Initialize/Finalize.
        return TAsyncLockReaderGuard::Acquire(&InitLock_)
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TReaderGuardPtr&& readerGuard) {
                    // Register this read request and collect futures of conflicting requests.
                    // The guard automatically unregisters the request and signals on destruction.
                    auto [guard, conflicts] = RegisterInflightRequest(offset, length, /*isWrite*/ false);

                    // Wait for conflicting requests to finish before issuing the read.
                    // We use AllSet (not AllSucceeded) so that a failed predecessor does not
                    // propagate its error into this request: we only need to wait for the range
                    // to be vacated, regardless of whether the conflicting request succeeded.
                    TWallTimer conflictWaitTimer;
                    return AllSet(std::move(conflicts))
                        .Apply(BIND(
                            [=,
                                guard = std::move(guard),
                                readerGuard = std::move(readerGuard),
                                conflictWaitTimer = std::move(conflictWaitTimer)
                            ] (const std::vector<TErrorOr<void>>& /*conflicts*/) mutable {
                                return TRequestPipeline{
                                    .Guard = std::move(guard),
                                    .ReaderGuard = std::move(readerGuard),
                                    .ConflictWaitDuration = conflictWaitTimer.GetElapsedTime(),
                                };
                            }))
                        // TRequestPipeline is move-only, so the inner future must be
                        // unique for the result to be moved (not copied) into the chain.
                        .AsUnique();
                }))
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TRequestPipeline&& pipeline) {
                    auto conflictWaitDuration = pipeline.ConflictWaitDuration;

                    TWallTimer throttleTimer;
                    return ReadThrottler_->Throttle(length)
                        .Apply(BIND(
                            [=,
                                pipeline = std::move(pipeline),
                                throttleTimer = std::move(throttleTimer),
                                this,
                                this_ = this_
                            ] (const TError& throttleError) mutable {
                                auto throttleWaitDuration = throttleTimer.GetElapsedTime();

                                if (!throttleError.IsOK()) {
                                    YT_LOG_WARNING(throttleError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, Cookie: %x)",
                                        offset,
                                        length,
                                        conflictWaitDuration,
                                        throttleWaitDuration,
                                        options.Cookie);

                                    pipeline.Guard.SetError(throttleError);
                                    THROW_ERROR throttleError;
                                }

                                pipeline.ThrottleWaitDuration = throttleWaitDuration;
                                return std::move(pipeline);
                            }))
                        // TRequestPipeline is move-only, so the inner future must be
                        // unique for the result to be moved (not copied) into the chain.
                        .AsUnique();
                }))
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TRequestPipeline&& pipeline) {
                    auto conflictWaitDuration = pipeline.ConflictWaitDuration;
                    auto throttleWaitDuration = pipeline.ThrottleWaitDuration;

                    TWallTimer rpcTimer;
                    return ChunkHandler_->Read(offset, length, options)
                        .Apply(BIND(
                            [=,
                                pipeline = std::move(pipeline),
                                rpcTimer = std::move(rpcTimer),
                                this,
                                this_ = this_
                            ] (const TErrorOr<TReadResponse>& rspOrError) mutable -> TReadResponse {
                                auto rpcWaitDuration = rpcTimer.GetElapsedTime();

                                if (!rspOrError.IsOK()) {
                                    YT_LOG_WARNING(rspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, RpcWaitDuration: %v, Cookie: %x)",
                                        offset,
                                        length,
                                        conflictWaitDuration,
                                        throttleWaitDuration,
                                        rpcWaitDuration,
                                        options.Cookie);

                                    pipeline.Guard.SetError(TError(rspOrError));
                                    THROW_ERROR rspOrError;
                                }

                                const auto& response = rspOrError.Value();

                                YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v, ShouldStopUsingDevice: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, RpcWaitDuration: %v, Cookie: %x)",
                                    offset,
                                    length,
                                    response.Data.Size(),
                                    response.ShouldStopUsingDevice,
                                    conflictWaitDuration,
                                    throttleWaitDuration,
                                    rpcWaitDuration,
                                    options.Cookie);

                                return response;
                                // pipeline (guard + reader lock) is destroyed here.
                            }));
                }));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        YT_LOG_DEBUG("Started writing to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        if (data.size() == 0) {
            YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);

            return MakeFuture<TWriteResponse>({});
        }

        // Acquire reader lock to prevent concurrent Initialize/Finalize.
        return TAsyncLockReaderGuard::Acquire(&InitLock_)
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TReaderGuardPtr&& readerGuard) {
                    // Register this write request and collect futures of all conflicting requests
                    // (both reads and writes that overlap with this range).
                    // The guard automatically unregisters the request and signals on destruction.
                    auto [guard, conflicts] = RegisterInflightRequest(offset, data.size(), /*isWrite*/ true);

                    // Wait for conflicting requests to finish before issuing the write.
                    // We use AllSet (not AllSucceeded) so that a failed predecessor does not
                    // propagate its error into this request: we only need to wait for the range
                    // to be vacated, regardless of whether the conflicting request succeeded.
                    TWallTimer conflictWaitTimer;
                    return AllSet(std::move(conflicts))
                        .Apply(BIND(
                            [=,
                                guard = std::move(guard),
                                readerGuard = std::move(readerGuard),
                                conflictWaitTimer = std::move(conflictWaitTimer)
                            ] (const std::vector<TErrorOr<void>>& /*conflicts*/) mutable {
                                return TRequestPipeline{
                                    .Guard = std::move(guard),
                                    .ReaderGuard = std::move(readerGuard),
                                    .ConflictWaitDuration = conflictWaitTimer.GetElapsedTime(),
                                };
                            }))
                        // TRequestPipeline is move-only, so the inner future must be
                        // unique for the result to be moved (not copied) into the chain.
                        .AsUnique();
                }))
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TRequestPipeline&& pipeline) {
                    auto conflictWaitDuration = pipeline.ConflictWaitDuration;

                    TWallTimer throttleTimer;
                    return WriteThrottler_->Throttle(data.size())
                        .Apply(BIND(
                            [=,
                                pipeline = std::move(pipeline),
                                throttleTimer = std::move(throttleTimer),
                                this,
                                this_ = this_
                            ] (const TError& throttleError) mutable {
                                auto throttleWaitDuration = throttleTimer.GetElapsedTime();

                                if (!throttleError.IsOK()) {
                                    YT_LOG_WARNING(throttleError, "Failed to write to chunk (Offset: %v, Length: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, Cookie: %x)",
                                        offset,
                                        data.size(),
                                        conflictWaitDuration,
                                        throttleWaitDuration,
                                        options.Cookie);

                                    pipeline.Guard.SetError(throttleError);
                                    THROW_ERROR throttleError;
                                }

                                pipeline.ThrottleWaitDuration = throttleWaitDuration;
                                return std::move(pipeline);
                            }))
                        // TRequestPipeline is move-only, so the inner future must be
                        // unique for the result to be moved (not copied) into the chain.
                        .AsUnique();
                }))
            .AsUnique()
            .Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (TRequestPipeline&& pipeline) {
                    auto conflictWaitDuration = pipeline.ConflictWaitDuration;
                    auto throttleWaitDuration = pipeline.ThrottleWaitDuration;

                    TWallTimer rpcTimer;
                    return ChunkHandler_->Write(offset, data, options)
                        .Apply(BIND(
                            [=,
                                pipeline = std::move(pipeline),
                                rpcTimer = std::move(rpcTimer),
                                this,
                                this_ = this_
                            ] (const TErrorOr<TWriteResponse>& rspOrError) mutable -> TWriteResponse {
                                auto rpcWaitDuration = rpcTimer.GetElapsedTime();

                                if (!rspOrError.IsOK()) {
                                    YT_LOG_WARNING(rspOrError, "Failed to write to chunk (Offset: %v, Length: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, RpcWaitDuration: %v, Cookie: %x)",
                                        offset,
                                        data.size(),
                                        conflictWaitDuration,
                                        throttleWaitDuration,
                                        rpcWaitDuration,
                                        options.Cookie);

                                    pipeline.Guard.SetError(TError(rspOrError));
                                    THROW_ERROR rspOrError;
                                }

                                const auto& response = rspOrError.Value();

                                YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, ShouldStopUsingDevice: %v, ConflictWaitDuration: %v, ThrottleWaitDuration: %v, RpcWaitDuration: %v, Cookie: %x)",
                                    offset,
                                    data.size(),
                                    response.ShouldStopUsingDevice,
                                    conflictWaitDuration,
                                    throttleWaitDuration,
                                    rpcWaitDuration,
                                    options.Cookie);

                                return response;
                            }));
                }));
    }

    TFuture<void> Flush() override
    {
        // Acquire reader lock so that Finalize() (which acquires the writer lock) cannot
        // tear down the chunk handler while an in-flight Flush() is still issuing RPCs.
        return TAsyncLockReaderGuard::Acquire(&InitLock_)
            .AsUnique()
            .Apply(BIND(
                [this, this_ = MakeStrong(this)] (TReaderGuardPtr&& readerGuard) mutable -> TFuture<void> {
                    return ChunkHandler_->Flush()
                        .Apply(BIND(
                            [this, this_ = MakeStrong(this), readerGuard = std::move(readerGuard)] (const TError& flushError) mutable -> TFuture<void> {
                                // Wait for in-flight requests regardless of flush outcome:
                                // the barrier must hold even if the handler flush failed.
                                return AllSet(CollectAllInflightFutures()).AsVoid()
                                    .Apply(BIND([flushError] () {
                                        if (!flushError.IsOK()) {
                                            THROW_ERROR flushError;
                                        }
                                    }));
                            }));
                }));
    }

    TFuture<void> Initialize() override
    {
        // Acquire writer lock: waits for all in-flight Read/Write/Flush operations to release
        // their reader locks, then blocks new ones until Initialize completes.
        return TAsyncLockWriterGuard::Acquire(&InitLock_)
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] (TIntrusivePtr<TAsyncLockWriterGuard> writerGuard) -> TFuture<void>
            {
                return ChunkHandler_->Initialize()
                    .Apply(BIND(
                        [writerGuard = std::move(writerGuard)] () {
                            // writerGuard is destroyed here, releasing the writer lock.
                        }));
            }));
    }

    TFuture<void> Finalize() override
    {
        // Acquire writer lock: waits for all in-flight Read/Write/Flush operations to release
        // their reader locks, then blocks new ones until Finalize completes.
        return TAsyncLockWriterGuard::Acquire(&InitLock_)
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] (TIntrusivePtr<TAsyncLockWriterGuard> writerGuard) -> TFuture<void>
            {
                return ChunkHandler_->Finalize()
                    .Apply(BIND(
                        [writerGuard = std::move(writerGuard)] () {
                            // writerGuard is destroyed here, releasing the writer lock.
                        }));
            }));
    }

private:
    static IChunkHandlerPtr CreateRequestHandler(
        TChunkBlockDevice* blockDevice,
        TChunkBlockDeviceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr channel,
        TSessionId sessionId,
        const TLogger& logger)
    {
        auto chunkHandler = CreateChunkHandler(
            blockDevice,
            config,
            invoker,
            std::move(channel),
            sessionId,
            logger);
        if (config->PageCache) {
            return New<TPageCache>(
                config->PageCache,
                std::move(chunkHandler),
                invoker,
                logger);
        }
        return chunkHandler;
    }

    const std::string ExportId_;
    const TChunkBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
    const IInvokerPtr Invoker_;
    static constexpr const char* SensorTag_ = "rw";
    const TLogger Logger;
    const IChunkHandlerPtr ChunkHandler_;

    //! Async RW lock protecting Initialize/Finalize from concurrent Read/Write:
    //! Read and Write acquire reader locks; Initialize and Finalize acquire the writer lock.
    TAsyncReaderWriterLock InitLock_;

    struct TInflightRequest
    {
        i64 Offset = 0;
        i64 Length = 0;
        bool IsWrite = false;
        TPromise<void> Done;
    };

    using TInflightList = std::list<TInflightRequest>;

    mutable YT_DECLARE_SPIN_LOCK(TSpinLock, InflightLock_);
    mutable TInflightList InflightRequests_;

    //! RAII guard that automatically unregisters an inflight request
    //! and signals its Done promise on destruction.
    class TInflightRequestGuard
    {
    public:
        TInflightRequestGuard(TInflightList* list, TSpinLock* lock, TInflightList::iterator it)
            : List_(list)
            , Lock_(lock)
            , It_(it)
        { }

        TInflightRequestGuard(TInflightRequestGuard&& other) noexcept
            : List_(other.List_)
            , Lock_(other.Lock_)
            , It_(other.It_)
            , Error_(std::move(other.Error_))
        {
            other.Moved_ = true;
        }

        ~TInflightRequestGuard()
        {
            if (Moved_) {
                return;
            }

            // Signal Done with the request outcome so that conflict waiters
            // can distinguish a failed request from a successful one.
            It_->Done.TrySet(Error_);

            // Unregister from the inflight list.
            auto guard = Guard(*Lock_);
            List_->erase(It_);
        }

        //! Record the request error so the destructor can propagate it to
        //! conflict waiters via the Done promise.
        void SetError(TError error)
        {
            Error_ = std::move(error);
        }

        // Non-copyable.
        TInflightRequestGuard(const TInflightRequestGuard&) = delete;
        TInflightRequestGuard& operator=(const TInflightRequestGuard&) = delete;
        TInflightRequestGuard& operator=(TInflightRequestGuard&&) = delete;

    private:
        TInflightList* const List_;
        TSpinLock* const Lock_;
        const TInflightList::iterator It_;
        bool Moved_ = false;
        TError Error_;
    };

    using TReaderGuardPtr = TIntrusivePtr<TAsyncLockReaderGuard>;

    //! State threaded between the flat request-pipeline stages (conflict-wait →
    //! throttle → RPC). Holds the RAII guards that must outlive the whole chain
    //! and the per-stage timings accumulated so far.
    struct TRequestPipeline
    {
        TInflightRequestGuard Guard;
        TReaderGuardPtr ReaderGuard;
        TDuration ConflictWaitDuration;
        TDuration ThrottleWaitDuration;
    };

    static bool Overlaps(i64 offset1, i64 length1, i64 offset2, i64 length2)
    {
        return offset1 < offset2 + length2 && offset2 < offset1 + length1;
    }

    //! Registers a new inflight request and returns:
    //! - a RAII guard that unregisters the request and signals on destruction.
    //! - a list of futures of conflicting in-flight requests to wait for.
    std::pair<TInflightRequestGuard, std::vector<TFuture<void>>> RegisterInflightRequest(
        i64 offset,
        i64 length,
        bool isWrite)
    {
        auto guard = Guard(InflightLock_);

        std::vector<TFuture<void>> conflicts;
        for (const auto& req : InflightRequests_) {
            if (!Overlaps(offset, length, req.Offset, req.Length)) {
                continue;
            }
            // read-read never conflicts.
            if (!isWrite && !req.IsWrite) {
                continue;
            }
            conflicts.push_back(req.Done.ToFuture());
        }

        InflightRequests_.push_back(TInflightRequest{
            .Offset = offset,
            .Length = length,
            .IsWrite = isWrite,
            .Done = NewPromise<void>(),
        });
        auto it = std::prev(InflightRequests_.end());

        return {TInflightRequestGuard(&InflightRequests_, &InflightLock_, it), std::move(conflicts)};
    }

    //! Collects futures of all currently inflight requests.
    std::vector<TFuture<void>> CollectAllInflightFutures() const
    {
        auto guard = Guard(InflightLock_);
        std::vector<TFuture<void>> futures;
        futures.reserve(InflightRequests_.size());
        for (const auto& req : InflightRequests_) {
            futures.push_back(req.Done.ToFuture());
        }
        return futures;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateChunkBlockDevice(
    std::string exportId,
    TChunkBlockDeviceConfigPtr config,
    IThroughputThrottlerPtr readThrottler,
    IThroughputThrottlerPtr writeThrottler,
    IInvokerPtr invoker,
    IChannelPtr channel,
    TSessionId sessionId,
    TLogger logger)
{
    return New<TChunkBlockDevice>(
        std::move(exportId),
        std::move(config),
        std::move(readThrottler),
        std::move(writeThrottler),
        std::move(invoker),
        std::move(channel),
        sessionId,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
