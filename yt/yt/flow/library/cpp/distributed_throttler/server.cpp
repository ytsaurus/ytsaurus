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
            logger ? logger : DefaultLogger)
        , Invoker_(std::move(invoker))
        , Profiler_(std::move(profiler))
        , ResponseKeeper_(CreateResponseKeeper(
            config->ResponseKeeper,
            NConcurrency::CreateSerializedInvoker(Invoker_),
            TServiceBase::Logger,
            Profiler_.WithPrefix("/response_keeper")))
    {
        // Cancelable so that an RPC timeout or a client cancellation reaches the
        // handler: ReplyFrom then forwards it to the bucket, which refunds the
        // request instead of granting quota nobody is waiting for any more.
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestQuota)
                .SetCancelable(true));

        ResponseKeeper_->Start();

        for (const auto& [name, bucketConfig] : config->Throttlers) {
            EmplaceOrCrash(Buckets_, name, CreateBucket(name, bucketConfig, config->DrainPeriod));
        }
    }

    TDistributedThrottlerBucketPtr CreateBucket(
        const std::string& name,
        const TDistributedThrottlerBucketConfigPtr& bucketConfig,
        TDuration drainPeriod)
    {
        auto bucket = New<TDistributedThrottlerBucket>(
            bucketConfig,
            drainPeriod,
            NConcurrency::CreateSerializedInvoker(Invoker_),
            TServiceBase::Logger.WithTag("Throttler", name),
            Profiler_.WithTag("throttler_id", TString(name)));
        bucket->Start();
        return bucket;
    }

    ~TDistributedThrottlerService() override
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

            for (const auto& [name, bucketConfig] : config->Throttlers) {
                auto it = Buckets_.find(name);
                if (it != Buckets_.end()) {
                    it->second->Reconfigure(bucketConfig);
                    it->second->SetDrainPeriod(config->DrainPeriod);
                } else {
                    EmplaceOrCrash(Buckets_, name, CreateBucket(name, bucketConfig, config->DrainPeriod));
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
    // Kept raw (possibly empty): the bucket resolves it to "default" and
    // separately counts classless requests on buckets with weighted classes.
    const auto& quotaClassId = request->quota_class_id();

    context->SetRequestInfo(
        "ThrottlerId: %v, ClientId: %v, Amount: %v, Timestamp: %v, QuotaClassId: %v",
        throttlerId,
        clientId,
        amount,
        timestamp,
        quotaClassId.empty() ? DefaultQuotaClassId : quotaClassId);

    // The token bucket YT_VERIFYs non-negative amounts; a malformed client
    // request must not abort the controller.
    THROW_ERROR_EXCEPTION_IF(
        amount < 0,
        "Quota amount must be non-negative, got %v",
        amount);

    // RPC-retry dedup by mutation id: the bucket sees each logical call once.
    if (ResponseKeeper_->TryReplyFrom(context)) {
        return;
    }

    auto bucket = FindBucket(throttlerId);
    if (!bucket) {
        context->Reply(TError("Unknown throttler %Qv", throttlerId));
        return;
    }

    auto future = bucket->RequestQuota(clientId, quotaClassId, amount, timestamp);

    // ReplyFrom also propagates context cancellation into the future. A plain
    // Subscribe would not: on an RPC timeout or a client cancellation the
    // request would stay queued, still consume its quota, and the client's
    // next prefetch would consume it a second time.
    context->ReplyFrom(std::move(future));
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
