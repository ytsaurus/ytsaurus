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

        LatestTimestampExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TRemoteTimestampProvider::SendUpdateRequest, MakeWeak(this)),
            Config_->UpdatePeriod,
            EPeriodicExecutorMode::Manual);
        LatestTimestampExecutor_->Start();
    }

    virtual TFuture<TErrorOr<TTimestamp>> GenerateTimestamps(int count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count > 0);

        TRequest request;
        request.Count = count;
        request.Promise = NewPromise<TErrorOr<TTimestamp>>();

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

        return LatestTimestamp_;
    }


private:
    TRemoteTimestampProviderConfigPtr Config_;

    std::unique_ptr<TTimestampServiceProxy> Proxy_;

    TPeriodicExecutorPtr LatestTimestampExecutor_;

    struct TRequest
    {
        int Count;
        TPromise<TErrorOr<TTimestamp>> Promise;
    };

    TSpinLock SpinLock_;
    bool GenerateInProgress_ = false;
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
        TTimestampServiceProxy::TRspGenerateTimestampsPtr rsp)
    {
        TError error;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            GenerateInProgress_ = false;

            if (rsp->IsOK()) {
                auto firstTimestamp = TTimestamp(rsp->timestamp());
                LatestTimestamp_ = firstTimestamp + requests.size();
                LOG_DEBUG("Fresh timestamps generated (Timestamps: %v-%v)",
                    firstTimestamp,
                    LatestTimestamp_);
            } else {
                error = TError("Error generating fresh timestamps") << *rsp;
                LOG_ERROR(error);
            }

            if (!PendingRequests_.empty()) {
                SendGenerateRequest(guard);
            }
        }

        if (error.IsOK()) {
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


    void SendUpdateRequest()
    {
        LOG_DEBUG("Updating current timestamp");

        auto req = Proxy_->GenerateTimestamps();
        req->Invoke().Subscribe(BIND(
            &TRemoteTimestampProvider::OnUpdateResponse,
            MakeStrong(this)));
    }

    void OnUpdateResponse(TTimestampServiceProxy::TRspGenerateTimestampsPtr rsp)
    {
        if (rsp->IsOK()) {
            auto timestamp = TTimestamp(rsp->timestamp());
            LOG_DEBUG("Current timestamp updated (Timestamp: %v)", timestamp);

            TGuard<TSpinLock> guard(SpinLock_);
            LatestTimestamp_ = std::max(LatestTimestamp_, timestamp);
        } else {
            LOG_WARNING(*rsp, "Error updating current timestamp");          
        }

        LatestTimestampExecutor_->ScheduleNext();
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

