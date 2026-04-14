#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NNbd {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockDevice
    : public TBaseBlockDevice
{
public:
    TChunkBlockDevice(
        TString exportId,
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
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
        , ChunkHandler_(CreateChunkHandler(
            this,
            Config_,
            Invoker_,
            std::move(channel),
            sessionId,
            Logger))
    { }

    ~TChunkBlockDevice()
    {
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

    TString DebugString() const override
    {
        return Format("{Size, %v}", GetTotalSize());
    }

    TString GetProfileSensorTag() const override
    {
        return TString();
    }

    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) override
    {
        // NB. For now causal dependency (i.e. read after write, write after write)
        // is resolved by making reads and writes serialized.
        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
            .ValueOrThrow();

        // Reject new requests if draining
        if (Draining_.load()) {
            YT_LOG_WARNING("Rejecting read request during drain (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);
            return MakeFuture<TReadResponse>(TError("Device is draining"));
        }

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

        YT_LOG_DEBUG("Started throttling read from chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            length,
            options.Cookie);

        auto throttleRspOrError = WaitFor(ReadThrottler_->Throttle(length));
        if (!throttleRspOrError.IsOK()) {
            YT_LOG_WARNING(throttleRspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);

            return MakeFuture<TReadResponse>(throttleRspOrError);
        }

        YT_LOG_DEBUG("Finished throttling read from chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            length,
            options.Cookie);

        auto future = BIND(&IChunkHandler::Read, ChunkHandler_, offset, length, options)
            .AsyncVia(Invoker_)
            .Run();

        auto rspOrError = WaitFor(future);
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to read from chunk (Offset: %v, ExpectedLength: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);

            return MakeFuture<TReadResponse>(rspOrError);
        }

        auto& response = rspOrError.Value();

        YT_LOG_DEBUG("Finished reading from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v, ShouldStopUsingDevice: %v, Cookie: %x)",
            offset,
            length,
            response.Data.Size(),
            response.ShouldStopUsingDevice,
            options.Cookie);

        return MakeFuture<TReadResponse>(std::move(response));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        // NB. For now causal dependency (i.e. read after write, write after write)
        // is resolved by making reads and writes serialized.
        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&Lock_))
            .ValueOrThrow();

        // Reject new requests if draining
        if (Draining_.load()) {
            YT_LOG_WARNING("Rejecting write request during drain (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);
            return MakeFuture<TWriteResponse>(TError("Device is draining"));
        }

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

        YT_LOG_DEBUG("Started throttling write to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        auto throttleRspOrError = WaitFor(WriteThrottler_->Throttle(data.size()));
        if (!throttleRspOrError.IsOK()) {
            YT_LOG_WARNING(throttleRspOrError, "Failed to write to chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);

            return MakeFuture<TWriteResponse>(throttleRspOrError);
        }

        YT_LOG_DEBUG("Finished throttling write to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        auto future = BIND(&IChunkHandler::Write, ChunkHandler_, offset, data, options)
            .AsyncVia(Invoker_)
            .Run();

        auto rspOrError = WaitFor(future);
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to write to chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);

            return MakeFuture<TWriteResponse>(rspOrError);
        }

        auto& response = rspOrError.Value();

        YT_LOG_DEBUG("Finished writing to chunk (Offset: %v, Length: %v, ShouldStopUsingDevice: %v, Cookie: %x)",
            offset,
            data.size(),
            response.ShouldStopUsingDevice,
            options.Cookie);

        return MakeFuture<TWriteResponse>(std::move(response));
    }

    TFuture<void> Flush() override
    {
        // Wait for pending requests to finish.
        return TAsyncLockWriterGuard::Acquire(&Lock_).AsVoid();
    }

    TFuture<void> Drain() override
    {
        YT_LOG_INFO("Draining device");

        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this)
                ] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& /*guard*/) {
                    if (Draining_.exchange(true)) {
                        YT_LOG_DEBUG("Draining is already in progress");
                        return DrainFuture_;
                    }

                    // Since we hold the write lock, no other request can be in progress.
                    DrainFuture_ = OKFuture;
                    YT_LOG_INFO("Drained device");
                    return DrainFuture_;
                }));
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
    const TString ExportId_;
    const TChunkBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    const IChunkHandlerPtr ChunkHandler_;

    //! All operations are serialized by write lock.
    TAsyncReaderWriterLock Lock_;
    std::atomic<bool> Draining_ = false;
    TFuture<void> DrainFuture_;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateChunkBlockDevice(
    TString exportId,
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
