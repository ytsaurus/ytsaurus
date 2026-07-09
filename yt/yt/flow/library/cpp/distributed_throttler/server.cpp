#include "server.h"

#include "bucket.h"
#include "service_proxy.h"

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NFlow::NDistributedThrottler {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger DefaultLogger("DistributedThrottler");

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerService
    : public TServiceBase
    , public IDistributedThrottlerService
{
public:
    TDistributedThrottlerService(
        TDistributedThrottlerServiceConfigPtr config,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler)
        : TServiceBase(
            invoker,
            TDistributedThrottlerServiceProxy::GetDescriptor(),
            logger ? std::move(logger) : DefaultLogger)
        , Invoker_(std::move(invoker))
        , Profiler_(std::move(profiler))
        , ResponseKeeper_(CreateResponseKeeper(
            config->ResponseKeeper,
            NConcurrency::CreateSerializedInvoker(Invoker_),
            TServiceBase::Logger,
            Profiler_.WithPrefix("/response_keeper")))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestQuota));

        ResponseKeeper_->Start();

        for (const auto& [name, throttlerConfig] : config->Throttlers) {
            EmplaceOrCrash(Buckets_, name, CreateBucket(name, throttlerConfig, config->DrainPeriod));
        }
    }

    TDistributedThrottlerBucketPtr CreateBucket(
        const std::string& name,
        const NConcurrency::TThroughputThrottlerConfigPtr& throttlerConfig,
        TDuration drainPeriod)
    {
        auto bucket = New<TDistributedThrottlerBucket>(
            throttlerConfig,
            drainPeriod,
            NConcurrency::CreateSerializedInvoker(Invoker_),
            TServiceBase::Logger.WithTag("Throttler: %v", name),
            Profiler_.WithTag("throttler_id", TString(name)));
        bucket->Start();
        return bucket;
    }

    ~TDistributedThrottlerService()
    {
        auto guard = WriterGuard(BucketsLock_);
        for (const auto& [_, bucket] : Buckets_) {
            bucket->Stop();
        }
    }

    NRpc::IServicePtr GetRpcService() override
    {
        return MakeStrong(static_cast<TServiceBase*>(this));
    }

    void Reconfigure(TDistributedThrottlerServiceConfigPtr config) override
    {
        std::vector<TDistributedThrottlerBucketPtr> toStop;
        std::vector<std::string> toRemove;
        {
            auto guard = WriterGuard(BucketsLock_);

            for (const auto& [name, throttlerConfig] : config->Throttlers) {
                auto it = Buckets_.find(name);
                if (it != Buckets_.end()) {
                    it->second->Reconfigure(throttlerConfig);
                    it->second->SetDrainPeriod(config->DrainPeriod);
                } else {
                    EmplaceOrCrash(Buckets_, name, CreateBucket(name, throttlerConfig, config->DrainPeriod));
                }
            }

            for (const auto& [name, bucket] : Buckets_) {
                if (!config->Throttlers.contains(name)) {
                    toRemove.push_back(name);
                    toStop.push_back(bucket);
                }
            }
            for (const auto& name : toRemove) {
                Buckets_.erase(name);
            }
        }

        for (const auto& bucket : toStop) {
            bucket->Stop();
        }
    }

    TDistributedThrottlerBucketPtr FindBucket(const std::string& name) const
    {
        auto guard = ReaderGuard(BucketsLock_);
        auto it = Buckets_.find(name);
        return it != Buckets_.end() ? it->second : nullptr;
    }

private:
    const IInvokerPtr Invoker_;
    const NProfiling::TProfiler Profiler_;
    const IResponseKeeperPtr ResponseKeeper_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BucketsLock_);
    THashMap<std::string, TDistributedThrottlerBucketPtr> Buckets_;

    DECLARE_RPC_SERVICE_METHOD(NProto, RequestQuota);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TDistributedThrottlerService, RequestQuota)
{
    const auto& throttlerId = request->throttler_id();
    const auto& clientId = request->client_id();
    auto amount = request->amount();
    auto timestamp = request->timestamp();

    context->SetRequestInfo(
        "ThrottlerId: %v, ClientId: %v, Amount: %v, Timestamp: %v",
        throttlerId,
        clientId,
        amount,
        timestamp);

    // RPC-retry dedup by mutation id: the bucket sees each logical call once.
    if (ResponseKeeper_->TryReplyFrom(context)) {
        return;
    }

    auto bucket = FindBucket(throttlerId);
    if (!bucket) {
        context->Reply(TError("Unknown throttler %Qv", throttlerId));
        return;
    }

    auto future = bucket->RequestQuota(clientId, amount, timestamp);

    future.Subscribe(BIND([context] (const TError& error) {
        if (error.IsOK()) {
            context->Reply();
        } else {
            context->Reply(error);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerServicePtr CreateDistributedThrottlerService(
    TDistributedThrottlerServiceConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    return New<TDistributedThrottlerService>(
        std::move(config),
        std::move(invoker),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
