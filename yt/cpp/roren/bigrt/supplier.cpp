#include "supplier.h"
#include <yt/yt/core/logging/log.h>
#include <bigrt/lib/utility/logging/logging.h>  // MainLogger
#include <bigrt/lib/supplier/common/integer_offsets.h>

namespace NRoren::NPrivate
{

static const auto& Logger = NBigRT::MainLogger;
const TString SYSTEM_TIMERS_SUPPLIER_NAME = "roren-system-timers";

TSystemTimersSupplier::TSystemTimersSupplier(const TSystemTimersSupplierConfig& config, const ui64 shard)
    : TSupplier(shard)
    , Config_(config)
    , Timers_(Config_.GetTimers(shard))
{
}

void TSystemTimersSupplier::CommitOffset(TStringBuf) noexcept
{
}

void TSystemTimersSupplier::SupplyUntilStopped(NBSYeti::TStopToken stopToken, TStringBuf startOffset, TCallback pushCallback) noexcept
{
    try {
        Offset_ = NBigRT::ParseIntegerOffsetOrFail<i64>(startOffset, -1);
        YT_LOG_INFO("TSystemTimersSupplier starts from offset: %v", Offset_);
        NBigRT::TMessageChunkPusher messageChunkPusher(
            ToString(Offset_),
            stopToken,
            pushCallback,
            {
                #if 0
                .MaxChunkSize = Config_.GetMaxOutChunkSize(),
                .FlushChunkInterval = TDuration::MilliSeconds(100),
                .FlushEmptyChunkInterval = TDuration::MilliSeconds(1000),
                .SkipSoftFlush = false,
                #endif
            }
        );
        const size_t limit = 128;  // it's a default value by @pechatnov
        while (!stopToken.IsSet()) {
            ++Offset_;
            auto readyTimers = Timers_.get().GetReadyTimers(limit);
            for (const auto& timer : readyTimers) {
                TString messageStr;
                Y_ABORT_UNLESS(timer.SerializeToString(&messageStr));
                messageChunkPusher.PushWithIntegerOffset({
                    .Data = messageStr,
                    .Offset = Offset_,
                });
            }
            messageChunkPusher.SoftFlush();
            if (readyTimers.size() < limit) {
                stopToken.Wait(TDuration::Seconds(1));
            }
        }
        messageChunkPusher.Flush();
        stopToken.Wait();
    } catch (...) {
        YT_LOG_FATAL("Supply forever fails: %v", NBSYeti::CurrentExceptionWithBacktrace());
    }
}

TSystemTimersSupplierFactory::TSystemTimersSupplierFactory(TSystemTimersSupplierConfig config)
    : TSupplierFactory()
    , Config_(std::move(config))
{
}

ui64 TSystemTimersSupplierFactory::GetShardsCount()
{
    return Config_.ShardsCount;
}

NBigRT::TSupplierPtr TSystemTimersSupplierFactory::CreateSupplier(ui64 shard)
{
    return NYT::New<TSystemTimersSupplier>(Config_, shard);
}

NBigRT::TSupplierFactoryPtr CreateSystemTimersSupplierFactory(TSystemTimersSupplierConfig config)
{
    return NYT::New<TSystemTimersSupplierFactory>(std::move(config));
}

NBigRT::TSupplierFactories CreateExSupplierFactories(NBigRT::TSupplierFactoriesData data, std::function<NPrivate::TTimers& (const ui64)> getTimers)
{
    auto factories = CreateSupplierFactories(std::move(data));
    i64 shardsCount = -1;
    for (const auto& kv : factories.GetMap()) {
        shardsCount = Max<i64>(shardsCount, kv.second->GetShardsCount());  // TODO: retry & stopToken
    }
    factories[SYSTEM_TIMERS_SUPPLIER_NAME] = CreateSystemTimersSupplierFactory({.GetTimers = getTimers, .ShardsCount = shardsCount});
    return factories;
}

//TODO: remove copy-paste
NBigRT::TSupplierFactoriesProvider CreateExSupplierFactoriesProvider(
    NBigRT::TSupplierFactoriesData data,
    std::function<NPrivate::TTimers& (const uint64_t, const NBigRT::TSupplierFactoryContext&)> getTimers)
{
    return [=](const NBigRT::TSupplierFactoryContext& context) mutable {
        if (nullptr == data.Context.YtClients) {
            data.Context.YtClients = context.YtClients;
        }
        if (nullptr == data.Context.GrutClientProvider) {
            data.Context.GrutClientProvider = context.GrutClientProvider;
        }
        if (data.Context.DefaultMainCluster.empty()) {
            data.Context.DefaultMainCluster = context.DefaultMainCluster;
        }
        if (data.Context.DefaultMainPath.empty()) {
            data.Context.DefaultMainPath = context.DefaultMainPath;
        }
        if (data.ProfilerTags.Tags().empty()) {
            data.ProfilerTags = context.ProfilerTags;
        }
        return CreateExSupplierFactories(data, [context = data.Context, getTimers](const ui64 shardId) -> NPrivate::TTimers& {
            return getTimers(shardId, context);
        });
    };
}

}  // namespace NRoren::NPrivate
