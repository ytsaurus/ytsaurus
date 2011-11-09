#pragma once

#include "../election/leader_lookup.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NMetaState {

// TODO: get rid
using NElection::TLeaderLookup;

////////////////////////////////////////////////////////////////////////////////

class TCellChannel
    : public NRpc::IChannel
{
public:
    typedef TIntrusivePtr<TCellChannel> TPtr;

    TCellChannel(const TLeaderLookup::TConfig& config);
    
    virtual TFuture<NRpc::TError>::TPtr Send(
        NRpc::IClientRequest::TPtr request,
        NRpc::IClientResponseHandler::TPtr responseHandler,
        TDuration timeout);

    virtual void Terminate();

private:
    DECLARE_ENUM(EState,
        (NotConnected)
        (Connecting)
        (Connected)
        (Failed)
        (Terminated)
    );

    TFuture<NRpc::TError>::TPtr OnGotChannel(
        NRpc::IChannel::TPtr channel,
        NRpc::IClientRequest::TPtr request,
        NRpc::IClientResponseHandler::TPtr responseHandler,
        TDuration timeout);

    NRpc::TError OnResponseReady(NRpc::TError error);
  
    TFuture<NRpc::IChannel::TPtr>::TPtr GetChannel();

    TFuture<NRpc::IChannel::TPtr>::TPtr OnFirstLookupResult(TLeaderLookup::TResult result);
    TFuture<NRpc::IChannel::TPtr>::TPtr OnSecondLookupResult(TLeaderLookup::TResult);


    TSpinLock SpinLock;
    TLeaderLookup::TPtr LeaderLookup;
    EState State;
    TFuture<TLeaderLookup::TResult>::TPtr LookupResult;
    NRpc::TChannel::TPtr Channel;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
