#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/rpc/balancing_channel.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteTimestampProvider(
        TRemoteTimestampProviderConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(config)
    {
        auto channel = CreateBalancingChannel(config, channelFactory);
        Proxy_ = std::make_unique<TTimestampServiceProxy>(channel);
        Proxy_->SetDefaultTimeout(Config_->RpcTimeout);
    }

    virtual TFuture<TTimestamp> GenerateTimestamps(int count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count > 0);

        TRequest request;
        request.Count = count;
        request.Promise = NewPromise<TTimestamp>();

        {
            TGuard<TSpinLock> guard(SpinLock_);
            PendingRequests_.push_back(request);
            if (!GenerateInProgress_) {
                YCHECK(PendingRequests_.size() == 1);
                SendGenerateRequest(guard);
            }
        }

        return request.Promise;
    }

    virtual TTimestamp GetLatestTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);

        auto result = LatestTimestamp_;

        if (!LatestTimestampExecutor_) {
            LatestTimestampExecutor_ = New<TPeriodicExecutor>(
                GetSyncInvoker(),
                BIND(&TRemoteTimestampProvider::UpdateLatestTimestamp, MakeWeak(this)),
                Config_->UpdatePeriod,
                EPeriodicExecutorMode::Automatic);
            guard.Release();
            LatestTimestampExecutor_->Start();
        }

        return result;
    }

private:
    const TRemoteTimestampProviderConfigPtr Config_;

    std::unique_ptr<TTimestampServiceProxy> Proxy_;

    struct TRequest
    {
        int Count;
        TPromise<TTimestamp> Promise;
    };

    TSpinLock SpinLock_;
    bool GenerateInProgress_ = false;
    TPeriodicExecutorPtr LatestTimestampExecutor_;
    TTimestamp LatestTimestamp_ = MinTimestamp;
    std::vector<TRequest> PendingRequests_;


    void SendGenerateRequest(TGuard<TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);
        YCHECK(!GenerateInProgress_);

        std::vector<TRequest> requests;
        requests.swap(PendingRequests_);

        int count = 0;
        for (const auto& request : requests) {
            count += request.Count;
        }

        LOG_DEBUG("Generating fresh timestamps (Count: %v)", count);

        auto req = Proxy_->GenerateTimestamps();
        req->set_count(count);

        GenerateInProgress_ = true;
        guard.Release();

        req->Invoke().Subscribe(BIND(
            &TRemoteTimestampProvider::OnGenerateResponse,
            MakeStrong(this),
            Passed(std::move(requests)),
            count));
    }

    void OnGenerateResponse(
        std::vector<TRequest> requests,
        int count,
        const TTimestampServiceProxy::TErrorOrRspGenerateTimestampsPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TError error;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            GenerateInProgress_ = false;

            if (rspOrError.IsOK()) {
                const auto& rsp = rspOrError.Value();
                auto firstTimestamp = TTimestamp(rsp->timestamp());
                LatestTimestamp_ = firstTimestamp + requests.size();
                LOG_DEBUG("Fresh timestamps generated (Timestamps: %v-%v)",
                    firstTimestamp,
                    LatestTimestamp_);
            } else {
                error = TError("Error generating fresh timestamps") << rspOrError;
                LOG_ERROR(error);
            }

            if (!PendingRequests_.empty()) {
                SendGenerateRequest(guard);
            }
        }

        if (error.IsOK()) {
            const auto& rsp = rspOrError.Value();
            auto timestamp = rsp->timestamp();
            for (auto& request : requests) {
                request.Promise.Set(timestamp);
                ++timestamp;
            }
        } else {
            for (auto& request : requests) {
                request.Promise.Set(error);
            }
        }
    }


    void UpdateLatestTimestamp()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_DEBUG("Updating latest timestamp");

        auto req = Proxy_->GenerateTimestamps();
        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error updating latest timestamp");
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto timestamp = TTimestamp(rsp->timestamp());

        TGuard<TSpinLock> guard(SpinLock_);
        if (timestamp > LatestTimestamp_) {
            LOG_DEBUG("Latest timestamp updated (Timestamp: %v)", timestamp);
            LatestTimestamp_ = timestamp;
        }
    }

};

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    return New<TRemoteTimestampProvider>(
        config,
        channelFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

