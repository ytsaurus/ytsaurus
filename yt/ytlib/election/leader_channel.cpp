#include "stdafx.h"
#include "leader_channel.h"

namespace NYT {
namespace NElection {

using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TResponseHandlerWrapper
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TResponseHandlerWrapper> TPtr;

    TResponseHandlerWrapper(
        IClientResponseHandler* underlyingHandler,
        IAction::TPtr onFailed)
        : UnderlyingHandler(underlyingHandler)
        , OnFailed(onFailed)
    { }

    virtual void OnAcknowledgement()
    {
        UnderlyingHandler->OnAcknowledgement();
    }

    virtual void OnResponse(IMessage* message)
    {
        UnderlyingHandler->OnResponse(message);
    }

    virtual void OnError(const TError& error)
    {
        UnderlyingHandler->OnError(error);

        auto code = error.GetCode();
        if (code == EErrorCode::Timeout ||
            code == EErrorCode::TransportError ||
            code == EErrorCode::Unavailable)
        {
            OnFailed->Do();
        }
    }

private:
    IClientResponseHandler::TPtr UnderlyingHandler;
    IAction::TPtr OnFailed;

};

////////////////////////////////////////////////////////////////////////////////

class TLeaderChannel
    : public IChannel
{
public:
    typedef TIntrusivePtr<TLeaderChannel> TPtr;

    TLeaderChannel(TLeaderLookup::TConfig* config)
        : Config(config)
        , LeaderLookup(New<TLeaderLookup>(config))
        , State(EState::NotConnected)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const
    {
        return Config->RpcTimeout;
    }

    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TNullable<TDuration> timeout)
    {
        YASSERT(request);
        YASSERT(responseHandler);
        YASSERT(State != EState::Terminated);

        GetChannel()->Subscribe(FromMethod(
            &TLeaderChannel::OnGotChannel,
            MakeStrong(this),
            request,
            responseHandler,
            timeout));
    }

    virtual void Terminate()
    {
        TGuard<TSpinLock> guard(SpinLock);
    
        if (State == EState::Terminated)
            return;

        if (Channel) {
            Channel->Terminate();
            Channel.Reset();
        }

        LookupResult.Reset();

        State = EState::Terminated;
    }

private:
    friend class TResponseHandlerWrapper;

    DECLARE_ENUM(EState,
        (NotConnected)
        (Connecting)
        (Connected)
        (Failed)
        (Terminated)
    );

    void OnGotChannel(
        IChannel::TPtr channel,
        IClientRequest::TPtr request,
        IClientResponseHandler::TPtr responseHandler,
        TNullable<TDuration> timeout)
    {
        if (!channel) {
            responseHandler->OnError(TError(
                EErrorCode::Unavailable,
                "Unable to determine the leader"));
        } else {
            auto responseHandlerWrapper = New<TResponseHandlerWrapper>(
                ~responseHandler,
                FromMethod(&TLeaderChannel::OnChannelFailed, MakeStrong(this)));
            channel->Send(~request, ~responseHandlerWrapper, timeout);
        }
    }

    void OnChannelFailed()
    {
        TGuard<TSpinLock> guard(SpinLock);
        switch (State) {
            case EState::Connected:
                State = EState::Failed;
                LookupResult.Reset();
                Channel.Reset();
                break;

            case EState::Connecting:
            case EState::Failed:
            case EState::Terminated:
                break;

            default:
                YUNREACHABLE();
        }
    }

    TFuture<IChannel::TPtr>::TPtr GetChannel()
    {
        TGuard<TSpinLock> guard(SpinLock);
        switch (State) {
            case EState::NotConnected:
            case EState::Failed: {
                YASSERT(!LookupResult);
                YASSERT(!Channel);
                State = EState::Connecting;
                auto lookupResult = LookupResult = LeaderLookup->GetLeader();
                guard.Release();

                return lookupResult->Apply(FromMethod(
                    &TLeaderChannel::OnFirstLookupResult,
                    MakeStrong(this)));
            }

            case EState::Connected:
                YASSERT(!LookupResult);
                YASSERT(Channel);
                return MakeFuture(Channel);

            case EState::Connecting: {
                YASSERT(LookupResult);
                YASSERT(!Channel);
                auto lookupResult = LookupResult;
                YASSERT(!lookupResult->IsSet());
                guard.Release();

                return lookupResult->Apply(FromMethod(
                    &TLeaderChannel::OnSecondLookupResult,
                    MakeStrong(this)));
            }

            case EState::Terminated:
                YASSERT(!LookupResult);
                YASSERT(!Channel);
                return NULL;

            default:
                YUNREACHABLE();
        }
    }

    TFuture<IChannel::TPtr>::TPtr OnFirstLookupResult(TLeaderLookup::TResult result)
    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(State == EState::Connecting);

        if (result.Id == NElection::InvalidPeerId) {
            State = EState::Failed;
            LookupResult.Reset();
            return MakeFuture(IChannel::TPtr(NULL));
        }

        State = EState::Connected;
        Channel = CreateBusChannel(result.Address);
        LookupResult.Reset();
        return MakeFuture(Channel);
    }

    TFuture<IChannel::TPtr>::TPtr OnSecondLookupResult(TLeaderLookup::TResult)
    {
        return GetChannel();
    }


    TLeaderLookup::TConfig::TPtr Config;
    TLeaderLookup::TPtr LeaderLookup;
    EState State;
    TSpinLock SpinLock;
    TFuture<TLeaderLookup::TResult>::TPtr LookupResult;
    IChannel::TPtr Channel;

};

IChannel::TPtr CreateLeaderChannel(TLeaderLookup::TConfig* config)
{
    return New<TLeaderChannel>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
