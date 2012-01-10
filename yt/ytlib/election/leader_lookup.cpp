#include "stdafx.h"
#include "leader_lookup.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("LeaderLookup");

NRpc::TChannelCache TLeaderLookup::ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TLeaderLookup::TLeaderLookup(TConfig* config)
    : Config(config)
{ }

TLeaderLookup::TAsyncResult::TPtr TLeaderLookup::GetLeader()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto asyncResult = New<TFuture<TResult> >();
    auto awaiter = New<TParallelAwaiter>();

    FOREACH(Stroka address, Config->Addresses) {
        LOG_DEBUG("Requesting leader from master %s", ~address);

        TProxy proxy(~ChannelCache.GetChannel(address));
        proxy.SetTimeout(Config->RpcTimeout);
        auto request = proxy.GetStatus();
        awaiter->Await(request->Invoke(), FromMethod(
            &TLeaderLookup::OnResponse,
            TPtr(this),
            awaiter,
            asyncResult,
            address));
    }
    
    awaiter->Complete(FromMethod(
        &TLeaderLookup::OnComplete,
        TPtr(this),
        asyncResult));

    return asyncResult;
}

void TLeaderLookup::OnResponse(
    TProxy::TRspGetStatus::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TFuture<TResult>::TPtr asyncResult,
    const Stroka& address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_WARNING("Error requesting leader from peer (Address: %s)\n%s",
            ~address,
            ~response->GetError().ToString());
        return;
    }

    auto voteId = response->vote_id();
    auto epoch = TEpoch::FromProto(response->vote_epoch());

    LOG_DEBUG("Received status from peer (Address: %s, PeerId: %d, State: %s, VoteId: %d, Priority: %" PRIx64 ", Epoch: %s)",
        ~address,
        response->self_id(),
        ~TProxy::EState(response->state()).ToString(),
        response->vote_id(),
        response->priority(),
        ~epoch.ToString());

    if (response->state() != TProxy::EState::Leading)
        return;

    TGuard<TSpinLock> guard(SpinLock);    
    if (asyncResult->IsSet())
        return;

    YASSERT(voteId == response->self_id());

    TResult result;
    result.Address = address;
    result.Id = voteId;
    result.Epoch = epoch;
    asyncResult->Set(result);

    awaiter->Cancel();

    LOG_INFO("Leader found (Address: %s, PeerId: %d, Epoch: %s)",
        ~address,
        response->self_id(),
        ~epoch.ToString());
}

void TLeaderLookup::OnComplete(TFuture<TResult>::TPtr asyncResult)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);    
    if (asyncResult->IsSet())
        return;

    TResult result;
    result.Address = "";
    result.Id = InvalidPeerId;
    result.Epoch = TEpoch();
    asyncResult->Set(result);

    LOG_INFO("No leader is found");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
