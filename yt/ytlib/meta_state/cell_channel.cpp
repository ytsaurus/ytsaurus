#include "stdafx.h"
#include "../misc/assert.h"
#include "cell_channel.h"

namespace NYT {
namespace NMetaState {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCellChannel::TCellChannel(const TLeaderLookup::TConfig& config)
    : LeaderLookup(New<TLeaderLookup>(config))
    , State(EState::NotConnected)
{ }

TFuture<TError>::TPtr TCellChannel::Send(
    IClientRequest::TPtr request,
    IClientResponseHandler::TPtr responseHandler,
    TDuration timeout)
{
    YASSERT(~request != NULL);
    YASSERT(~responseHandler != NULL);
    YASSERT(State != EState::Terminated);

    return GetChannel()->Apply(FromMethod(
        &TCellChannel::OnGotChannel,
        TPtr(this),
        request,
        responseHandler,
        timeout));
}

void TCellChannel::Terminate()
{
    TGuard<TSpinLock> guard(SpinLock);
    
    if (State == EState::Terminated)
        return;

    if (~Channel != NULL) {
        Channel->Terminate();
        Channel.Reset();
    }

    LookupResult.Reset();

    State = EState::Terminated;
}

TFuture<TError>::TPtr TCellChannel::OnGotChannel(
    IChannel::TPtr channel,
    IClientRequest::TPtr request,
    IClientResponseHandler::TPtr responseHandler,
    TDuration timeout)
{
    if (~channel == NULL) {
        responseHandler->OnAcknowledgement(NBus::IBus::ESendResult::Failed);
        return New< TFuture<TError> >(TError(EErrorCode::Unavailable));
    }

    return
        channel
        ->Send(request, responseHandler, timeout)
        ->Apply(FromMethod(
            &TCellChannel::OnResponseReady,
            TPtr(this)));
}

TError TCellChannel::OnResponseReady(TError error)
{
    auto code = error.GetCode();
    if (code == EErrorCode::TransportError ||
        code == EErrorCode::Timeout ||
        code == EErrorCode::Unavailable)
    {
        TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Terminated) {
            State = EState::Failed;
            LookupResult.Reset();
            Channel.Reset();
        }
    }
    return error;
}

TFuture<IChannel::TPtr>::TPtr TCellChannel::GetChannel()
{
    TGuard<TSpinLock> guard(SpinLock);
    switch (State) {
        case EState::NotConnected:
        case EState::Failed: {
            YASSERT(~LookupResult == NULL);
            YASSERT(~Channel == NULL);
            State = EState::Connecting;
            auto lookupResult = LookupResult = LeaderLookup->GetLeader();
            guard.Release();

            return lookupResult->Apply(FromMethod(
                &TCellChannel::OnFirstLookupResult,
                TPtr(this)));
        }

        case EState::Connected:
            YASSERT(~LookupResult == NULL);
            YASSERT(~Channel != NULL);
            return New< TFuture<IChannel::TPtr> >(~Channel);

        case EState::Connecting: {
            YASSERT(~LookupResult != NULL);
            YASSERT(~Channel == NULL);
            auto lookupResult = LookupResult;
            guard.Release();

            return lookupResult->Apply(FromMethod(
                &TCellChannel::OnSecondLookupResult,
                TPtr(this)));
        }

        case EState::Terminated:
            YASSERT(~LookupResult == NULL);
            YASSERT(~Channel == NULL);
            return NULL;

        default:
            YUNREACHABLE();
    }
}

TFuture<IChannel::TPtr>::TPtr TCellChannel::OnFirstLookupResult(
    TLeaderLookup::TResult result)
{
    TGuard<TSpinLock> guard(SpinLock);

    YASSERT(State == EState::Connecting);

    if (result.Id == NElection::InvalidPeerId) {
        State = EState::Failed;
        LookupResult.Reset();
        return New< TFuture<IChannel::TPtr> >(IChannel::TPtr(NULL));
    }

    State = EState::Connected;
    Channel = New<TChannel>(result.Address);
    LookupResult.Reset();
    return New< TFuture<IChannel::TPtr> >(~Channel);
}

TFuture<IChannel::TPtr>::TPtr TCellChannel::OnSecondLookupResult(
    TLeaderLookup::TResult)
{
    return GetChannel();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
