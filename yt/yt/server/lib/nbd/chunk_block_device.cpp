#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"

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
        YT_LOG_DEBUG("Start read from chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            length,
            options.Cookie);

        if (length == 0) {
            YT_LOG_DEBUG("Finish read from chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);

            return MakeFuture<TReadResponse>({});
        }

        // NB. For now causal dependancy (i.e. read after write, write after write)
        // is resolved by making reads and writes synchronous.
        WaitFor(ReadThrottler_->Throttle(length)).ThrowOnError();
        auto response = WaitFor(ChunkHandler_->Read(offset, length, options)).ValueOrThrow();

        YT_LOG_DEBUG("Finish read from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v, Cookie: %x)",
            offset,
            length,
            response.Data.Size(),
            options.Cookie);

        return MakeFuture<TReadResponse>(std::move(response));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        YT_LOG_DEBUG("Start write to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        if (data.size() == 0) {
            YT_LOG_DEBUG("Finish write to chunk (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);

            return MakeFuture<TWriteResponse>({});
        }

        // NB. For now causal dependancy (i.e. read after write, write after write)
        // is resolved by making reads and writes synchronous.
        WaitFor(WriteThrottler_->Throttle(data.size())).ThrowOnError();
        auto response = WaitFor(ChunkHandler_->Write(offset, data, options)).ValueOrThrow();

        YT_LOG_DEBUG("Finish write to chunk (Offset: %v, Length: %v, Cookie: %x)",
            offset,
            data.size(),
            options.Cookie);

        return MakeFuture<TWriteResponse>(std::move(response));
    }

    TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    TFuture<void> Initialize() override
    {
        return ChunkHandler_->Initialize();
    }

    TFuture<void> Finalize() override
    {
        return ChunkHandler_->Finalize();
    }

private:
    const TString ExportId_;
    const TChunkBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    const IChunkHandlerPtr ChunkHandler_;
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
