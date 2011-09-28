#include "cell_channel.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TCellChannel::TCellChannel(const TLeaderLookup::TConfig& config)
    : LeaderLookup(New<TLeaderLookup>(config))
    , State(EState::NotConnected)
{ }

TFuture<TVoid>::TPtr TCellChannel::Send(
    NRpc::TClientRequest::TPtr request,
    NRpc::TClientResponse::TPtr response,
    TDuration timeout)
{
    YASSERT(~request != NULL);
    YASSERT(~response != NULL);

    return GetChannel()->Apply(FromMethod(
        &TCellChannel::OnGotChannel,
        TPtr(this),
        request,
        response,
        timeout));
}

TFuture<TVoid>::TPtr TCellChannel::OnGotChannel(
    NRpc::IChannel::TPtr channel,
    NRpc::TClientRequest::TPtr request,
    NRpc::TClientResponse::TPtr response,
    TDuration timeout)
{
    if (~channel == NULL) {
        response->OnAcknowledgement(NBus::IBus::ESendResult::Failed);
        return New< TFuture<TVoid> >(TVoid());
    }

    return
        channel
        ->Send(request, response, timeout)
        ->Apply(FromMethod(
            &TCellChannel::OnResponseReady,
            TPtr(this),
            response));
}

NYT::TVoid TCellChannel::OnResponseReady(
    TVoid,
    NRpc::TClientResponse::TPtr response)
{
    auto errorCode = response->GetErrorCode();
    if (errorCode == NRpc::EErrorCode::TransportError ||
        errorCode == NRpc::EErrorCode::Timeout ||
        errorCode == NRpc::EErrorCode::Unavailable)
    {
        TGuard<TSpinLock> guard(SpinLock);
        State = EState::Failed;
        LookupResult = NULL;
        Channel = NULL;
    }
    return TVoid();
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

        default:
            YASSERT(false);
            return NULL;
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
