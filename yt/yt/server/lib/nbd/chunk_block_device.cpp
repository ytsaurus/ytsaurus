#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"
#include "profiler.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NRpc;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockDevice
    : public TBaseBlockDevice
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
        : ExportId_(exportId)
        , Config_(std::move(config))
        , ReadThrottler_(std::move(readThrottler))
        , WriteThrottler_(std::move(writeThrottler))
        , Invoker_(std::move(invoker))
        , SensorTag_("rw")
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , ChunkHandler_(CreateChunkHandler(
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

    bool IsReadOnly() const override
    {
        return false;
    }

    std::string DebugString() const override
    {
        return Format("{Size, %v}", GetTotalSize());
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

        // Register this read request and collect futures of conflicting writes.
        // The guard automatically unregisters the request and signals on destruction.
        auto [guard, conflicts] = RegisterInflightRequest(offset, length, /*isWrite*/ false);

        TWallTimer conflictWaitTimer;
        // Wait for conflicting requests → throttle → RPC.
        return AllSucceeded(std::move(conflicts))
            .Apply(BIND(
                [=,
                    guard = std::move(guard),
                    conflictWaitTimer = std::move(conflictWaitTimer),
                    this,
                    this_ = MakeStrong(this)
                ] (const TError& conflictError) mutable -> TFuture<TReadResponse>
            {
                auto conflictWaitDuration = conflictWaitTimer.GetElapsedTime();

                if (!conflictError.IsOK()) {
                    guard.SetError(conflictError);

                    YT_LOG_WARNING(conflictError, "Failed to read from chunk while waiting for conflicts (Offset: %v, Length: %v, ConflictWaitDuration: %v, Cookie: %x)",
                        offset,
                        length,
                        conflictWaitDuration,
                        options.Cookie);

                    THROW_ERROR conflictError;
                }

                TWallTimer throttleTimer;
                return ReadThrottler_->Throttle(length)
                    .Apply(BIND(
                        [=,
                            guard = std::move(guard),
                            throttleTimer = std::move(throttleTimer),
                            this,
                            this_ = this_
                        ] (const TError& throttleError) mutable -> TFuture<TReadResponse>
                    {
                        auto throttleDuration = throttleTimer.GetElapsedTime();

                        if (!throttleError.IsOK()) {
                            guard.SetError(throttleError);

                            YT_LOG_WARNING(throttleError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, Cookie: %x)",
                                offset,
                                length,
                                conflictWaitDuration,
                                throttleDuration,
                                options.Cookie);

                            THROW_ERROR throttleError;
                        }

                        TWallTimer rpcTimer;
                        return ChunkHandler_->Read(offset, length, options)
                            .Apply(BIND(
                                [=,
                                    guard = std::move(guard),
                                    rpcTimer = std::move(rpcTimer),
                                    this,
                                    this_ = this_
                                ] (const TErrorOr<TReadResponse>& rspOrError) mutable -> TReadResponse
                            {
                                auto rpcDuration = rpcTimer.GetElapsedTime();

                                if (!rspOrError.IsOK()) {
                                    guard.SetError(rspOrError);

                                    YT_LOG_WARNING(rspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                                        offset,
                                        length,
                                        conflictWaitDuration,
                                        throttleDuration,
                                        rpcDuration,
                                        options.Cookie);

                                    THROW_ERROR rspOrError;
                                }

                                const auto& response = rspOrError.Value();

                                YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v, ShouldStopUsingDevice: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                                    offset,
                                    length,
                                    response.Data.Size(),
                                    response.ShouldStopUsingDevice,
                                    conflictWaitDuration,
                                    throttleDuration,
                                    rpcDuration,
                                    options.Cookie);

                                return response;
                                // guard is destroyed here: unregisters the request and signals Done.
                            }));
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

        // Register this write request and collect futures of all conflicting requests
        // (both reads and writes that overlap with this range).
        // The guard automatically unregisters the request and signals on destruction.
        auto [guard, conflicts] = RegisterInflightRequest(offset, data.size(), /*isWrite*/ true);

        TWallTimer conflictWaitTimer;
        // Wait for conflicting requests → throttle → RPC.
        return AllSucceeded(std::move(conflicts))
            .Apply(BIND(
                [=,
                    guard = std::move(guard),
                    conflictWaitTimer = std::move(conflictWaitTimer),
                    this,
                    this_ = MakeStrong(this)
                ] (const TError& conflictError) mutable -> TFuture<TWriteResponse>
            {
                auto conflictWaitDuration = conflictWaitTimer.GetElapsedTime();

                if (!conflictError.IsOK()) {
                    guard.SetError(conflictError);

                    YT_LOG_WARNING(conflictError, "Failed to write to chunk while waiting for conflicts (Offset: %v, Length: %v, ConflictWaitDuration: %v, Cookie: %x)",
                        offset,
                        data.size(),
                        conflictWaitDuration,
                        options.Cookie);

                    THROW_ERROR conflictError;
                }

                TWallTimer throttleTimer;
                return WriteThrottler_->Throttle(data.size())
                    .Apply(BIND(
                        [=,
                            guard = std::move(guard),
                            throttleTimer = std::move(throttleTimer),
                            this,
                            this_ = this_
                        ] (const TError& throttleError) mutable -> TFuture<TWriteResponse>
                    {
                        auto throttleDuration = throttleTimer.GetElapsedTime();

                        if (!throttleError.IsOK()) {
                            guard.SetError(throttleError);

                            YT_LOG_WARNING(throttleError, "Failed to write to chunk (Offset: %v, Length: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, Cookie: %x)",
                                offset,
                                data.size(),
                                conflictWaitDuration,
                                throttleDuration,
                                options.Cookie);

                            THROW_ERROR throttleError;
                        }

                        TWallTimer rpcTimer;
                        return ChunkHandler_->Write(offset, data, options)
                            .Apply(BIND(
                                [=,
                                    guard = std::move(guard),
                                    rpcTimer = std::move(rpcTimer),
                                    this,
                                    this_ = this_
                                ] (const TErrorOr<TWriteResponse>& rspOrError) mutable -> TWriteResponse
                            {
                                auto rpcDuration = rpcTimer.GetElapsedTime();

                                if (!rspOrError.IsOK()) {
                                    guard.SetError(rspOrError);

                                    YT_LOG_WARNING(rspOrError, "Failed to write to chunk (Offset: %v, Length: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                                        offset,
                                        data.size(),
                                        conflictWaitDuration,
                                        throttleDuration,
                                        rpcDuration,
                                        options.Cookie);

                                    THROW_ERROR rspOrError;
                                }

                                const auto& response = rspOrError.Value();

                                YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, ShouldStopUsingDevice: %v, ConflictWaitDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                                    offset,
                                    data.size(),
                                    response.ShouldStopUsingDevice,
                                    conflictWaitDuration,
                                    throttleDuration,
                                    rpcDuration,
                                    options.Cookie);

                                return response;
                                // guard is destroyed here: unregisters the request and signals Done.
                            }));
                    }));
            }));
    }

    TFuture<void> Flush() override
    {
        // Wait for all pending requests to finish.
        return AllSucceeded(CollectAllInflightFutures());
    }

    TFuture<void> Initialize() override
    {
        return AllSucceeded(CollectAllInflightFutures())
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] () {
                    return ChunkHandler_->Initialize();
                }));
    }

    TFuture<void> Finalize() override
    {
        return AllSucceeded(CollectAllInflightFutures())
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] () {
                    return ChunkHandler_->Finalize();
                }));
    }

private:
    const std::string ExportId_;
    const TChunkBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
    const IInvokerPtr Invoker_;
    const std::string SensorTag_;
    const TLogger Logger;
    const IChunkHandlerPtr ChunkHandler_;

    struct TInflightRequest
    {
        i64 Offset = 0;
        i64 Length = 0;
        bool IsWrite = false;
        TPromise<void> Done;
    };

    using TInflightList = std::list<TInflightRequest>;

    YT_DECLARE_SPIN_LOCK(TSpinLock, InflightLock_);
    TInflightList InflightRequests_;

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
        {
            other.Moved_ = true;
        }

        ~TInflightRequestGuard()
        {
            if (Moved_) {
                return;
            }

            // Signal Done (with success if not already set with an error).
            It_->Done.TrySet();

            // Unregister from the inflight list.
            auto guard = Guard(*Lock_);
            List_->erase(It_);
        }

        // Non-copyable.
        TInflightRequestGuard(const TInflightRequestGuard&) = delete;
        TInflightRequestGuard& operator=(const TInflightRequestGuard&) = delete;
        TInflightRequestGuard& operator=(TInflightRequestGuard&&) = delete;

        //! Propagates an error to waiters before the guard is destroyed.
        void SetError(const TError& error)
        {
            It_->Done.TrySet(error);
        }

    private:
        TInflightList* const List_;
        TSpinLock* const Lock_;
        const TInflightList::iterator It_;
        bool Moved_ = false;
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
    std::vector<TFuture<void>> CollectAllInflightFutures()
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

} // namespace NYT::NNbd
