#include "session_coordinator.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>
#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/private.h>

namespace NYT::NQueryClient {

using namespace NCompression;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NThreading;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

struct TPingableDistributedSessionContext final
{
    TDistributedSessionId SessionId;
    ECodec CodecId;
    TDistributedSessionOptions Options;
};

DEFINE_REFCOUNTED_TYPE(TPingableDistributedSessionContext)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodePinger)

class TNodePinger
    : public TRefCounted
{
public:
    TNodePinger(
        IInvokerPtr invoker,
        TWeakPtr<IDistributedSessionCoordinator> coordinator,
        IChannelPtr channel,
        TIntrusivePtr<TPingableDistributedSessionContext> context)
        : Invoker_(std::move(invoker))
        , Coordinator_(std::move(coordinator))
        , Proxy_(std::move(channel))
        , PingExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TNodePinger::SendPing, MakeWeak(this)),
            context->Options.PingPeriod))
        , Context_(std::move(context))
    {
        PingExecutor_->Start();
    }

    void Close()
    {
        auto req = Proxy_.CloseDistributedSession();
        ToProto(req->mutable_session_id(), Context_->SessionId);
        req->SetTimeout(Context_->Options.ControlRpcTimeout);
        YT_UNUSED_FUTURE(req->Invoke());

        YT_UNUSED_FUTURE(PingExecutor_->Stop());
    }

private:
    const IInvokerPtr Invoker_;
    const TWeakPtr<IDistributedSessionCoordinator> Coordinator_;
    const TQueryServiceProxy Proxy_;
    const TPeriodicExecutorPtr PingExecutor_;

    TIntrusivePtr<TPingableDistributedSessionContext> Context_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, PropagateSpinLock_);
    THashSet<TString> PropagatedAddresses_;

    void SendPing()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto req = Proxy_.PingDistributedSession();
        ToProto(req->mutable_session_id(), Context_->SessionId);
        ToProto(req->mutable_nodes_with_propagated_session(), GrabPropagatedAddresses());
        req->SetTimeout(Context_->Options.ControlRpcTimeout);
        req->Invoke()
            .Subscribe(BIND(&TNodePinger::OnSessionPinged, MakeWeak(this)).Via(Invoker_));
    }

    void OnSessionPinged(const TQueryServiceProxy::TErrorOrRspPingDistributedSessionPtr& rspOrError)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto coordinator = Coordinator_.Lock();
        if (!coordinator) {
            return;
        }

        if (rspOrError.IsOK()) {
            auto rsp = rspOrError.Value();

            auto addresses = FromProto<std::vector<TString>>(rsp->nodes_to_propagate_session_onto());

            for (const auto& address : addresses) {
                coordinator->BindToRemoteSession(address);
            }

            RegisterPropagatedAddresses(std::move(addresses));
        } else {
            coordinator->Abort(rspOrError);
        }
    }

    std::vector<TString> GrabPropagatedAddresses()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        THashSet<TString> addressesSet;
        {
            auto guard = Guard(PropagateSpinLock_);
            std::swap(PropagatedAddresses_, addressesSet);
        }

        std::vector<TString> addresses;
        addresses.reserve(addressesSet.size());
        std::move(addressesSet.begin(), addressesSet.end(), std::back_inserter(addresses));
        return addresses;
    }

    void RegisterPropagatedAddresses(std::vector<TString> addresses)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
        IInvokerPtr invoker,
        INodeChannelFactoryPtr channelFactory,
        ECodec codedId,
        TDistributedSessionOptions options)
        : Invoker_(std::move(invoker))
        , NodeChannelFactory_(std::move(channelFactory))
        , Context_(New<TPingableDistributedSessionContext>(
            TDistributedSessionId::Create(),
            codedId,
            options))
    { }

    TDistributedSessionId GetDistributedSessionId() const override
    {
        return Context_->SessionId;
    }

    void BindToRemoteSession(TString address) override
    {
        {
            auto guard = Guard(SessionCoordinatorLock_);
            if (!SessionMembers_.insert(address).second){
                return;
            }
        }

        DoBindToRemoteSession(std::move(address));
    }

    void Abort(TError error) override
    {
        if (AbortByPingFailPromise_.ToFuture().Cancel(error)) {
            YT_LOG_DEBUG(error, "Distributed query session execution cancelled (SessionId: %v)",
                Context_->SessionId);
        }
    }

    void CloseRemoteSessions() override
    {
        YT_LOG_DEBUG("Closing remote distributed query session instances (SessionId: %v)",
            Context_->SessionId);

        for (auto& pinger : Pingers_) {
            pinger->Close();
        }

        {
            auto guard = Guard(SessionCoordinatorLock_);
            Pingers_.clear();
        }
    }

private:
    const IInvokerPtr Invoker_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TPromise<void> AbortByPingFailPromise_ = NewPromise<void>();
    TIntrusivePtr<TPingableDistributedSessionContext> Context_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SessionCoordinatorLock_);
    THashSet<TString> SessionMembers_;
    std::vector<TNodePingerPtr> Pingers_;

    TFuture<void> CreateSessionInstanceIfNecessary(TQueryServiceProxy proxy, TString address)
    {
        THROW_ERROR_EXCEPTION_IF(AbortByPingFailPromise_.IsSet(),
            "Cannot bind to a closed distributed query session coordinator");

        {
            auto guard = Guard(SessionCoordinatorLock_);
            if (!SessionMembers_.insert(address).second) {
                return VoidFuture;
            }
        }

        auto req = proxy.CreateDistributedSession();
        ToProto(req->mutable_session_id(), Context_->SessionId);
        req->set_retention_time(ToProto<i64>(Context_->Options.RetentionTime));
        req->set_codec(ToProto<i32>(Context_->CodecId));
        req->SetTimeout(Context_->Options.ControlRpcTimeout);

        return req->Invoke()
            .AsVoid()
            .Apply(BIND(
                &TDistributedSessionCoordinator::DoBindToRemoteSession,
                MakeWeak(this),
                std::move(address)));
    }

    void DoBindToRemoteSession(TString address)
    {
        auto pinger = New<TNodePinger>(
            Invoker_,
            MakeWeak(this),
            NodeChannelFactory_->CreateChannel(address),
            Context_);

        {
            auto guard = Guard(SessionCoordinatorLock_);
            Pingers_.push_back(std::move(pinger));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedSessionCoordinator)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionCoordinatorPtr CreateDistributeSessionCoordinator(
    IInvokerPtr invoker,
    INodeChannelFactoryPtr channelFactory,
    ECodec codecId,
    TDistributedSessionOptions options)
{
    return New<TDistributedSessionCoordinator>(
        std::move(invoker),
        std::move(channelFactory),
        codecId,
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
