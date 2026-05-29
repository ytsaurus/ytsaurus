#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"
#include "profiler.h"

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/profiling/timing.h>

namespace NYT::NNbd {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NRpc;

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

        // NB. For now causal dependency (i.e. read after write, write after write)
        // is resolved by making reads and writes serialized.
        TWallTimer lockTimer;
        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
            .ValueOrThrow();
        auto lockDuration = lockTimer.GetElapsedTime();

        if (length == 0) {
            YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, Length: %v, LockDuration: %v, Cookie: %x)",
                offset,
                length,
                lockDuration,
                options.Cookie);

            return MakeFuture<TReadResponse>({});
        }

        TWallTimer throttleTimer;
        auto throttleRspOrError = WaitFor(ReadThrottler_->Throttle(length));
        auto throttleDuration = throttleTimer.GetElapsedTime();

        if (!throttleRspOrError.IsOK()) {
            YT_LOG_WARNING(throttleRspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, LockDuration: %v, ThrottleDuration: %v, Cookie: %x)",
                offset,
                length,
                lockDuration,
                throttleDuration,
                options.Cookie);

            return MakeFuture<TReadResponse>(throttleRspOrError);
        }

        auto future = BIND(&IChunkHandler::Read, ChunkHandler_, offset, length, options)
            .AsyncVia(Invoker_)
            .Run();

        TWallTimer rpcTimer;
        auto rspOrError = WaitFor(future);
        auto rpcDuration = rpcTimer.GetElapsedTime();

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, LockDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                offset,
                length,
                lockDuration,
                throttleDuration,
                rpcDuration,
                options.Cookie);

            return MakeFuture<TReadResponse>(rspOrError);
        }

        auto& response = rspOrError.Value();

        YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v, ShouldStopUsingDevice: %v, LockDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
            offset,
            length,
            response.Data.Size(),
            response.ShouldStopUsingDevice,
            lockDuration,
            throttleDuration,
            rpcDuration,
            options.Cookie);

        return MakeFuture<TReadResponse>(std::move(response));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        YT_LOG_DEBUG("Started writing to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        // NB. For now causal dependency (i.e. read after write, write after write)
        // is resolved by making reads and writes serialized.
        TWallTimer lockTimer;
        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
            .ValueOrThrow();
        auto lockDuration = lockTimer.GetElapsedTime();

        if (data.size() == 0) {
            YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, LockDuration: %v, Cookie: %x)",
                offset,
                data.size(),
                lockDuration,
                options.Cookie);

            return MakeFuture<TWriteResponse>({});
        }

        TWallTimer throttleTimer;
        auto throttleRspOrError = WaitFor(WriteThrottler_->Throttle(data.size()));
        auto throttleDuration = throttleTimer.GetElapsedTime();

        if (!throttleRspOrError.IsOK()) {
            YT_LOG_WARNING(throttleRspOrError, "Failed to write to chunk (Offset: %v, Length: %v, LockDuration: %v, ThrottleDuration: %v, Cookie: %x)",
                offset,
                data.size(),
                lockDuration,
                throttleDuration,
                options.Cookie);

            return MakeFuture<TWriteResponse>(throttleRspOrError);
        }

        auto future = BIND(&IChunkHandler::Write, ChunkHandler_, offset, data, options)
            .AsyncVia(Invoker_)
            .Run();

        TWallTimer rpcTimer;
        auto rspOrError = WaitFor(future);
        auto rpcDuration = rpcTimer.GetElapsedTime();

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to write to chunk (Offset: %v, Length: %v, LockDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
                offset,
                data.size(),
                lockDuration,
                throttleDuration,
                rpcDuration,
                options.Cookie);

            return MakeFuture<TWriteResponse>(rspOrError);
        }

        auto& response = rspOrError.Value();

        YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, ShouldStopUsingDevice: %v, LockDuration: %v, ThrottleDuration: %v, RpcDuration: %v, Cookie: %x)",
            offset,
            data.size(),
            response.ShouldStopUsingDevice,
            lockDuration,
            throttleDuration,
            rpcDuration,
            options.Cookie);

        return MakeFuture<TWriteResponse>(std::move(response));
    }

    TFuture<void> Flush() override
    {
        // Wait for pending requests to finish.
        return TAsyncLockWriterGuard::Acquire(&Lock_).AsVoid();
    }

    TFuture<void> Initialize() override
    {
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& /*guard*/) {
                    return ChunkHandler_->Initialize();
                }));
    }

    TFuture<void> Finalize() override
    {
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& /*guard*/) {
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

    //! All operations are serialized by write lock.
    TAsyncReaderWriterLock Lock_;
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
