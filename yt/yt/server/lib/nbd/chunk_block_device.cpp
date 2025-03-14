#include "chunk_block_device.h"

#include "chunk_handler.h"
#include "config.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NNbd {

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockDevice
    : public IBlockDevice
{
public:
    TChunkBlockDevice(
        TChunkBlockDeviceConfigPtr config,
        IThroughputThrottlerPtr readThrottler,
        IThroughputThrottlerPtr writeThrottler,
        IInvokerPtr invoker,
        IChannelPtr channel,
        TLogger logger)
        : Config_(std::move(config))
        , ReadThrottler_(std::move(readThrottler))
        , WriteThrottler_(std::move(writeThrottler))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
        , ChunkHandler_(CreateChunkHandler(
            Config_,
            Invoker_,
            std::move(channel),
            Logger))
    { }

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

    TFuture<TSharedRef> Read(i64 offset, i64 length) override
    {
        YT_LOG_DEBUG("Start read from chunk (Offset: %v, Length: %v)",
            offset,
            length);

        if (length == 0) {
            YT_LOG_DEBUG("Finish read from chunk (Offset: %v, Length: %v)",
                offset,
                length);
            return MakeFuture<TSharedRef>({});
        }

        // NB. For now causal dependancy (i.e. read after write, write after write)
        // is resolved by making reads and writes synchronous.
        WaitFor(ReadThrottler_->Throttle(length)).ThrowOnError();
        auto data = WaitFor(ChunkHandler_->Read(offset, length)).ValueOrThrow();

        YT_LOG_DEBUG("Finish read from chunk (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
            offset,
            length,
            data.Size());
        return MakeFuture<TSharedRef>(data);
    }

    TFuture<void> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        YT_LOG_DEBUG("Start write to chunk (Offset: %v, Length: %v)",
            offset,
            data.size());

        if (data.size() == 0) {
            YT_LOG_DEBUG("Finish write to chunk (Offset: %v, Length: %v)",
                offset,
                data.size());
            return VoidFuture;
        }

        // NB. For now causal dependancy (i.e. read after write, write after write)
        // is resolved by making reads and writes synchronous.
        WaitFor(WriteThrottler_->Throttle(data.size())).ThrowOnError();
        WaitFor(ChunkHandler_->Write(offset, data, options)).ThrowOnError();

        YT_LOG_DEBUG("Finish write to chunk (Offset: %v, Length: %v)",
            offset,
            data.size());
        return VoidFuture;
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
    const TChunkBlockDeviceConfigPtr Config_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    const IChunkHandlerPtr ChunkHandler_;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateChunkBlockDevice(
    TChunkBlockDeviceConfigPtr config,
    IThroughputThrottlerPtr readThrottler,
    IThroughputThrottlerPtr writeThrottler,
    IInvokerPtr invoker,
    IChannelPtr channel,
    TLogger logger)
{
    return New<TChunkBlockDevice>(
        std::move(config),
        std::move(readThrottler),
        std::move(writeThrottler),
        std::move(invoker),
        std::move(channel),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
