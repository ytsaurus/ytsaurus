#include "../misc/stdafx.h"
#include "../misc/assert.h"
#include "cell_channel.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TCellChannel::TCellChannel(const TLeaderLookup::TConfig& config)
    : LeaderLookup(New<TLeaderLookup>(config))
    , State(EState::NotConnected)
{ }

TFuture<NRpc::EErrorCode>::TPtr TCellChannel::Send(
    NRpc::TClientRequest::TPtr request,
    NRpc::IClientResponseHandler::TPtr responseHandler,
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
        Channel.Drop();
    }

    LookupResult.Drop();

    State = EState::Terminated;
}

TFuture<NRpc::EErrorCode>::TPtr TCellChannel::OnGotChannel(
    NRpc::IChannel::TPtr channel,
    NRpc::TClientRequest::TPtr request,
    NRpc::IClientResponseHandler::TPtr responseHandler,
    TDuration timeout)
{
    if (~channel == NULL) {
        responseHandler->OnAcknowledgement(NBus::IBus::ESendResult::Failed);
        return New< TFuture<NRpc::EErrorCode> >(NRpc::EErrorCode::Unavailable);
    }

    return
        channel
        ->Send(request, responseHandler, timeout)
        ->Apply(FromMethod(
            &TCellChannel::OnResponseReady,
            TPtr(this)));
}

NRpc::EErrorCode TCellChannel::OnResponseReady(NRpc::EErrorCode errorCode)
{
    if (errorCode == NRpc::EErrorCode::TransportError ||
        errorCode == NRpc::EErrorCode::Timeout ||
        errorCode == NRpc::EErrorCode::Unavailable)
    {
        TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Terminated) {
            State = EState::Failed;
            LookupResult.Drop();
            Channel.Drop();
        }
    }
    return errorCode;
}

TFuture<NRpc::IChannel::TPtr>::TPtr TCellChannel::GetChannel()
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
            return New< TFuture<NRpc::IChannel::TPtr> >(~Channel);

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

TFuture<NRpc::IChannel::TPtr>::TPtr TCellChannel::OnFirstLookupResult(
    TLeaderLookup::TResult result)
{
    TGuard<TSpinLock> guard(SpinLock);

    YASSERT(State == EState::Connecting);

    if (result.Id == NElection::InvalidPeerId) {
        State = EState::Failed;
        LookupResult.Drop();
        return New< TFuture<NRpc::IChannel::TPtr> >(NRpc::IChannel::TPtr(NULL));
    }

    State = EState::Connected;
    Channel = New<NRpc::TChannel>(result.Address);
    LookupResult.Drop();
    return New< TFuture<NRpc::IChannel::TPtr> >(~Channel);
}

TFuture<NRpc::IChannel::TPtr>::TPtr TCellChannel::OnSecondLookupResult(
    TLeaderLookup::TResult)
{
    return GetChannel();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
