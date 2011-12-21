#include "stdafx.h"
#include "../misc/assert.h"
#include "cell_channel.h"

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
        IAction* onFailed)
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

class TCellChannel
    : public IChannel
{
public:
    typedef TIntrusivePtr<TCellChannel> TPtr;

    TCellChannel(TLeaderLookup::TConfig* config)
        : LeaderLookup(New<TLeaderLookup>(config))
        , State(EState::NotConnected)
    { }
    
    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TDuration timeout)
    {
        YASSERT(request);
        YASSERT(responseHandler);
        YASSERT(State != EState::Terminated);

        GetChannel()->Subscribe(FromMethod(
            &TCellChannel::OnGotChannel,
            TPtr(this),
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
        TDuration timeout)
    {
        if (!channel) {
            responseHandler->OnError(TError(
                EErrorCode::Unavailable,
                "Unable to determine the leader"));
        } else {
            auto responseHandlerWrapper = New<TResponseHandlerWrapper>(
                ~responseHandler,
                ~FromMethod(&TCellChannel::OnChannelFailed, TPtr(this)));
            channel->Send(~request, ~responseHandlerWrapper, timeout);
        }
    }

    void OnChannelFailed()
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State != EState::Terminated) {
            State = EState::Failed;
            LookupResult.Reset();
            Channel.Reset();
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
                    &TCellChannel::OnFirstLookupResult,
                    TPtr(this)));
            }

            case EState::Connected:
                YASSERT(!LookupResult);
                YASSERT(Channel);
                return ToFuture(Channel);

            case EState::Connecting: {
                YASSERT(LookupResult);
                YASSERT(!Channel);
                auto lookupResult = LookupResult;
                guard.Release();

                return lookupResult->Apply(FromMethod(
                    &TCellChannel::OnSecondLookupResult,
                    TPtr(this)));
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
            return ToFuture(IChannel::TPtr(NULL));
        }

        State = EState::Connected;
        Channel = CreateBusChannel(result.Address);
        LookupResult.Reset();
        return ToFuture(Channel);
    }

    TFuture<IChannel::TPtr>::TPtr OnSecondLookupResult(TLeaderLookup::TResult)
    {
        return GetChannel();
    }


    TSpinLock SpinLock;
    TLeaderLookup::TPtr LeaderLookup;
    EState State;
    TFuture<TLeaderLookup::TResult>::TPtr LookupResult;
    IChannel::TPtr Channel;

};

IChannel::TPtr CreateCellChannel(TLeaderLookup::TConfig* config)
{
    return New<TCellChannel>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
