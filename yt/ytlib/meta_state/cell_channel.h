#pragma once

#include "../election/leader_lookup.h"
#include "../rpc/channel.h"

namespace NYT {

using NElection::TLeaderLookup;

////////////////////////////////////////////////////////////////////////////////

class TCellChannel
    : public NRpc::IChannel
{
public:
    typedef TIntrusivePtr<TCellChannel> TPtr;

    TCellChannel(const TLeaderLookup::TConfig& config);
    
    virtual TAsyncResult<TVoid>::TPtr Send(
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

    TAsyncResult<TVoid>::TPtr OnGotChannel(
        NRpc::IChannel::TPtr channel,
        NRpc::TClientRequest::TPtr request,
        NRpc::TClientResponse::TPtr response,
        TDuration timeout);

    TVoid OnResponseReady(
        TVoid,
        NRpc::TClientResponse::TPtr response);
  
    TAsyncResult<NRpc::IChannel::TPtr>::TPtr GetChannel();

    TAsyncResult<NRpc::IChannel::TPtr>::TPtr OnFirstLookupResult(TLeaderLookup::TResult result);
    TAsyncResult<NRpc::IChannel::TPtr>::TPtr OnSecondLookupResult(TLeaderLookup::TResult);


    TSpinLock SpinLock;
    TLeaderLookup::TPtr LeaderLookup;
    EState State;
    TAsyncResult<TLeaderLookup::TResult>::TPtr LookupResult;
    NRpc::TChannel::TPtr Channel;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
