#include "dns_over_rpc_resolver.h"

#include "config.h"
#include "helpers.h"
#include "dns_over_rpc_service_proxy.h"

#include <yt/yt/core/dns/dns_resolver.h>
#include <yt/yt/core/dns/private.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NDns {

using namespace NConcurrency;
using namespace NNet;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DnsLogger;

////////////////////////////////////////////////////////////////////////////////

class TDnsOverRpcResolver
    : public IDnsResolver
{
public:
    TDnsOverRpcResolver(
        TDnsOverRpcResolverConfigPtr config,
        IChannelPtr channel)
        : Config_(std::move(config))
        , Channel_(std::move(channel))
    { }

    TFuture<TNetworkAddress> Resolve(
        const std::string& hostName,
        const TDnsResolveOptions& options)
    {
        auto guard = Guard(Lock_);

        auto* subrequest = Subrequests_.Add();
        subrequest->set_host_name(ToProto(hostName));
        ToProto(subrequest->mutable_options(), options);

        auto promise = NewPromise<TNetworkAddress>();
        Promises_.push_back(promise);

        if (!BatchingTimeoutCookie_) {
            BatchingTimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(&TDnsOverRpcResolver::FlushBatch, MakeStrong(this)),
                Config_->ResolveBatchingPeriod,
                NRpc::TDispatcher::Get()->GetLightInvoker());
        }

        YT_TLOG_DEBUG("Resolve request batched")
            .With("HostName", hostName)
            .With("Options", options);

        return promise.ToFuture();
    }

private:
    const TDnsOverRpcResolverConfigPtr Config_;

    const NYT::NBus::IBusClientPtr BusClient_;
    const IChannelPtr Channel_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TDelayedExecutorCookie BatchingTimeoutCookie_;
    google::protobuf::RepeatedPtrField<NProto::TReqResolve_TSubrequest> Subrequests_;
    std::vector<TPromise<TNetworkAddress>> Promises_;

    void FlushBatch()
    {
        TDnsOverRpcServiceProxy proxy(Channel_);

        auto req = proxy.Resolve();
        req->SetTimeout(Config_->ResolveRpcTimeout);

        auto guard = Guard(Lock_);
        *req->mutable_subrequests() = std::move(Subrequests_);
        Subrequests_.Clear();
        auto promises = std::exchange(Promises_, {});
        BatchingTimeoutCookie_ = {};
        guard.Release();

        YT_TLOG_DEBUG("Started resolving batched requests")
            .With("Count", req->subrequests_size());

        req->Invoke()
            .Subscribe(BIND(&TDnsOverRpcResolver::OnBatchResolved, MakeStrong(this), Passed(std::move(promises)))
                .Via(NRpc::TDispatcher::Get()->GetLightInvoker()));
    }

    void OnBatchResolved(
        std::vector<TPromise<TNetworkAddress>> promises,
        const TDnsOverRpcServiceProxy::TErrorOrRspResolvePtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_TLOG_WARNING("Error resolving batched requests")
                .With(rspOrError);
            auto error = TError("DNS-over-RPC resolve failed")
                .With(rspOrError);
            for (const auto& promise : promises) {
                promise.Set(error);
            }
            return;
        }

        YT_TLOG_DEBUG("Finished resolving batched requests");

        const auto& rsp = rspOrError.Value();
        YT_VERIFY(std::ssize(promises) == rsp->subresponses_size());
        for (int index = 0; index < std::ssize(promises); ++index) {
            const auto& promise = promises[index];
            const auto& subresponse = rsp->subresponses(index);
            if (subresponse.has_error()) {
                promise.Set(FromProto<TError>(subresponse.error()));
            } else {
                promise.Set(FromProto<TNetworkAddress>(subresponse.address()));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDnsResolverPtr CreateDnsOverRpcResolver(
    TDnsOverRpcResolverConfigPtr config,
    IChannelPtr channel)
{
    return New<TDnsOverRpcResolver>(
        std::move(config),
        std::move(channel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
