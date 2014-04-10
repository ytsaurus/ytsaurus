#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>

#include <core/rpc/balancing_channel.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteTimestampProvider(
        TRemoteTimestampProviderConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(config)
        , Channel_(CreateBalancingChannel(config, channelFactory))
        , Proxy_(Channel_)
        , RequestInProgress_(false)
        , LatestTimestamp_(MinTimestamp)
    { }

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
            if (!RequestInProgress_) {
                YCHECK(PendingRequests_.size() == 1);
                SendRequest(guard);
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

    IChannelPtr Channel_;
    TTimestampServiceProxy Proxy_;

    struct TRequest
    {
        int Count;
        TPromise<TErrorOr<TTimestamp>> Promise;
    };

    TSpinLock SpinLock_;
    bool RequestInProgress_;
    volatile TTimestamp LatestTimestamp_;
    std::vector<TRequest> PendingRequests_;


    void SendRequest(TGuard<TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);
        YCHECK(!RequestInProgress_);

        std::vector<TRequest> requests;
        requests.swap(PendingRequests_);

        int count = 0;
        for (const auto& request : requests) {
            count += request.Count;
        }

        auto req = Proxy_.GenerateTimestamps();
        req->set_count(count);

        RequestInProgress_ = true;
        guard.Release();

        req->Invoke().Subscribe(BIND(
            &TRemoteTimestampProvider::OnResponse,
            MakeStrong(this),
            Passed(std::move(requests))));
    }

    void OnResponse(
        std::vector<TRequest> requests,
        TTimestampServiceProxy::TRspGenerateTimestampsPtr rsp)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        RequestInProgress_ = false;

        if (rsp->IsOK()) {
            auto timestamp = TTimestamp(rsp->timestamp());
            for (auto& request : requests) {
                request.Promise.Set(TErrorOr<TTimestamp>(timestamp));
                timestamp += request.Count;
            }
        } else {
            for (auto& request : requests) {
                request.Promise.Set(rsp->GetError());
            }
        }

        if (!PendingRequests_.empty()) {
            SendRequest(guard);
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

