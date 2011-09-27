#pragma once

#include "../election/leader_lookup.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NMetaState {

using NElection::TLeaderLookup;

////////////////////////////////////////////////////////////////////////////////

class TCellChannel
    : public NRpc::IChannel
{
public:
    typedef TIntrusivePtr<TCellChannel> TPtr;

    TCellChannel(const TLeaderLookup::TConfig& config);
    
    virtual TFuture<TVoid>::TPtr Send(
        TIntrusivePtr<NRpc::TClientRequest> request,
        TIntrusivePtr<NRpc::TClientResponse> response,
        TDuration timeout);

private:
    DECLARE_ENUM(EState,
        (NotConnected)
        (Connecting)
        (Connected)
        (Failed)
    );

    TFuture<TVoid>::TPtr OnGotChannel(
        NRpc::IChannel::TPtr channel,
        NRpc::TClientRequest::TPtr request,
        NRpc::TClientResponse::TPtr response,
        TDuration timeout);

    TVoid OnResponseReady(
        TVoid,
        NRpc::TClientResponse::TPtr response);
  
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
