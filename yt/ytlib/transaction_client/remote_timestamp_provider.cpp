#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/rpc/balancing_channel.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteTimestampProvider(
        TRemoteTimestampProviderConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(config)
        , GenerateInProgress_(false)
        , LatestTimestamp_(MinTimestamp)
    {
        auto channel = CreateBalancingChannel(config, channelFactory);
        Proxy_ = std::make_unique<TTimestampServiceProxy>(channel);
        Proxy_->SetDefaultTimeout(Config_->RpcTimeout);

        LatestTimestampExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TRemoteTimestampProvider::SendGetRequest, MakeWeak(this)),
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
    bool GenerateInProgress_;
    TTimestamp LatestTimestamp_;
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

        auto req = Proxy_->GenerateTimestamps();
        req->set_count(count);

        GenerateInProgress_ = true;
        guard.Release();

        req->Invoke().Subscribe(BIND(
            &TRemoteTimestampProvider::OnGenerateResponse,
            MakeStrong(this),
            Passed(std::move(requests))));
    }

    void OnGenerateResponse(
        std::vector<TRequest> requests,
        TTimestampServiceProxy::TRspGenerateTimestampsPtr rsp)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        GenerateInProgress_ = false;

        if (rsp->IsOK()) {
            auto latestTimestamp = LatestTimestamp_;
            auto currentTimestamp = TTimestamp(rsp->timestamp());
            for (auto& request : requests) {
                request.Promise.Set(TErrorOr<TTimestamp>(currentTimestamp));
                currentTimestamp += request.Count;
                latestTimestamp = std::max(latestTimestamp, currentTimestamp - 1);
            }
            LatestTimestamp_ = latestTimestamp;
        } else {
            for (auto& request : requests) {
                request.Promise.Set(rsp->GetError());
            }
        }

        if (!PendingRequests_.empty()) {
            SendGenerateRequest(guard);
        }
    }


    void SendGetRequest()
    {
        auto req = Proxy_->GetTimestamp();
        req->Invoke().Subscribe(BIND(
            &TRemoteTimestampProvider::OnGetResponse,
            MakeStrong(this)));
    }

    void OnGetResponse(TTimestampServiceProxy::TRspGetTimestampPtr rsp)
    {
        if (rsp->IsOK()) {
            TGuard<TSpinLock> guard(SpinLock_);
            auto timestamp = TTimestamp(rsp->timestamp());
            LatestTimestamp_ = std::max(LatestTimestamp_, timestamp);
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

