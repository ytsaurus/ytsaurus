#include "session_coordinator.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>
#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/query/base/query.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NThreading;

using NYT::ToProto;
using NYT::FromProto;

static const TLogger Logger("DistributedQuerySession");

////////////////////////////////////////////////////////////////////////////////

struct TSessionCommon
    : public TRefCounted
{
    TSessionId Id;
    TSessionOptions Options;
    IInvokerPtr Invoker;

    TSessionCommon(
        TSessionId id,
        TSessionOptions options,
        IInvokerPtr invoker)
        : Id(id)
        , Options(options)
        , Invoker(std::move(invoker))
    { }
};

DEFINE_REFCOUNTED_TYPE(TSessionCommon)

////////////////////////////////////////////////////////////////////////////////

class TNodePinger
    : public TRefCounted
{
public:
    TNodePinger(
        TIntrusivePtr<const TSessionCommon> common,
        TWeakPtr<IDistributedSessionCoordinator> coordinator,
        TQueryServiceProxy proxy)
        : Common_(std::move(common))
        , Coordinator_(std::move(coordinator))
        , Proxy_(std::move(proxy))
        , PingExecutor_(New<TPeriodicExecutor>(
            common->Invoker,
            BIND(&TNodePinger::SendPing, MakeWeak(this)),
            Common_->Options.PingPeriod))
    {
        PingExecutor_->Start();
    }

    void Close()
    {
        auto req = Proxy_.CloseSessionInstance();
        ToProto(req->mutable_session_id(), Common_->Id);
        YT_UNUSED_FUTURE(req->Invoke());

        YT_UNUSED_FUTURE(PingExecutor_->Stop());
    }

private:
    const TIntrusivePtr<const TSessionCommon> Common_;
    const TWeakPtr<IDistributedSessionCoordinator> Coordinator_;
    const TQueryServiceProxy Proxy_;
    const TPeriodicExecutorPtr PingExecutor_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, PropagateSpinLock_);
    THashSet<TString> PropagatedAddresses_;

    void SendPing()
    {
        auto req = Proxy_.PingSessionInstance();
        ToProto(req->mutable_session_id(), Common_->Id);
        ToProto(req->mutable_nodes_with_propagated_session(), GrabPropagatedAddresses());
        req->SetTimeout(Common_->Options.ControlRpcTimeout);
        req->Invoke()
            .Subscribe(BIND(&TNodePinger::HandlePingResponse, MakeWeak(this)));
    }

    void HandlePingResponse(const TQueryServiceProxy::TErrorOrRspPingSessionInstancePtr& rspOrError)
    {
        auto coordinator = Coordinator_.Lock();
        if (!coordinator) {
            return;
        }

        if (rspOrError.IsOK()) {
            auto rsp = rspOrError.Value();

            auto addresses = FromProto<std::vector<TString>>(rsp->nodes_to_propagate_session_onto());

            for (const auto& address : addresses) {
                coordinator->BindToRemoteSessionIfNecessary(address);
            }

            RegisterPropagatedAddresses(std::move(addresses));
        } else {
            coordinator->PingAbortCallback(rspOrError);
        }
    }

    std::vector<TString> GrabPropagatedAddresses()
    {
        THashSet<TString> swap;
        {
            auto guard = Guard(PropagateSpinLock_);
            std::swap(PropagatedAddresses_, swap);
        }

        std::vector<TString> addresses;
        addresses.reserve(swap.size());
        std::move(swap.begin(), swap.end(), std::back_inserter(addresses));
        return addresses;
    }

    void RegisterPropagatedAddresses(std::vector<TString> addresses)
    {
        auto guard = Guard(PropagateSpinLock_);

        for (auto& address : addresses) {
            PropagatedAddresses_.insert(std::move(address));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TNodePinger)

////////////////////////////////////////////////////////////////////////////////

class TDistributedSessionCoordinator
    : public IDistributedSessionCoordinator
{
public:
    TDistributedSessionCoordinator(
        TSessionOptions options,
        IInvokerPtr invoker,
        INodeChannelFactoryPtr channelFactory)
        : Common_(New<TSessionCommon>(
            TSessionId::Create(),
            options,
            std::move(invoker)))
        , NodeChannelFactory_(std::move(channelFactory))
        , AbortByPingFailPromise_(NewPromise<void>())
    { }

    TSessionId GetId() const override
    {
        return Common_->Id;
    }

    void BindToRemoteSessionIfNecessary(TString address) override
    {
        THROW_ERROR_EXCEPTION_IF(AbortByPingFailPromise_.IsSet(),
            "Cannot bind to a disbanded distributed query session coordinator");

        {
            auto guard = Guard(SessionCoordinatorLock_);
            if (!SessionMembers_.insert(address).second){
                return;
            }
        }

        BindToRemoteSessionInstance(TQueryServiceProxy(NodeChannelFactory_->CreateChannel(std::move(address))));
    }

    void PingAbortCallback(TError error) override
    {
        if (AbortByPingFailPromise_.ToFuture().Cancel(error)) {
            YT_LOG_DEBUG(error,
                "Distributed query session execution cancelled (SessionId: %v)",
                Common_->Id);
        }
    }

    void CloseRemoteInstances() override
    {
        YT_LOG_INFO("Closing remote distributed query session instances (SessionId: %v)",
            Common_->Id);

        for (auto& node : Pingers_) {
            node->Close();
        }

        Pingers_.clear();
    }

private:
    const TIntrusivePtr<const TSessionCommon> Common_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TPromise<void> AbortByPingFailPromise_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SessionCoordinatorLock_);
    THashSet<TString> SessionMembers_;
    std::vector<TNodePingerPtr> Pingers_;

    TFuture<void> CreateSessionInstanceIfNecessary(TQueryServiceProxy proxy, TString address)
    {
        THROW_ERROR_EXCEPTION_IF(AbortByPingFailPromise_.IsSet(),
            "Cannot bind to a disbanded distributed query session coordinator");

        {
            auto guard = Guard(SessionCoordinatorLock_);
            if (!SessionMembers_.insert(std::move(address)).second) {
                return VoidFuture;
            }
        }

        auto req = proxy.CreateSessionInstance();
        ToProto(req->mutable_session_id(), Common_->Id);
        req->set_retention_time(ToProto<i64>(Common_->Options.RetentionTime));

        return req->Invoke()
            .AsVoid()
            .Apply(BIND(&TDistributedSessionCoordinator::BindToRemoteSessionInstance, MakeWeak(this), std::move(proxy)));
    }

    void BindToRemoteSessionInstance(TQueryServiceProxy proxy)
    {
        auto pinger = New<TNodePinger>(Common_, MakeWeak(this), std::move(proxy));

        {
            auto guard = Guard(SessionCoordinatorLock_);
            Pingers_.push_back(std::move(pinger));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedSessionCoordinator)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionCoordinatorPtr CreateDistributeSessionCoordinator(
    TSessionOptions options,
    IInvokerPtr invoker,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory)
{
    return New<TDistributedSessionCoordinator>(options, std::move(invoker), std::move(channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
