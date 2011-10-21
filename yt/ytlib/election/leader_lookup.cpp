#include "../misc/stdafx.h"
#include "leader_lookup.h"

#include "../misc/serialize.h"
#include "../misc/thread_affinity.h"
#include "../logging/log.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("LeaderLookup");

NRpc::TChannelCache TLeaderLookup::ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TLeaderLookup::TLeaderLookup(const TConfig& config)
    : Config(config)
{ }

TFuture<TLeaderLookup::TResult>::TPtr TLeaderLookup::GetLeader()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto asyncResult = New<TFuture<TResult> >();
    auto awaiter = New<TParallelAwaiter>();

    FOREACH(Stroka address, Config.Addresses) {
        LOG_DEBUG("Requesting leader from master %s", ~address);

        TProxy proxy(~ChannelCache.GetChannel(address));
        auto request = proxy.GetStatus();
        awaiter->Await(request->Invoke(Config.RpcTimeout), FromMethod(
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
    Stroka address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_WARNING("Error requesting leader from peer (Address: %s, ErrorCode: %s)",
            ~address,
            ~response->GetErrorCode().ToString());
        return;
    }

    auto voteId = response->GetVoteId();
    auto epoch = TEpoch::FromProto(response->GetVoteEpoch());

    LOG_DEBUG("Received status from peer (Address: %s, Id: %d, State: %s, VoteId: %d, Priority: %" PRIx64 ", Epoch: %s)",
        ~address,
        response->GetSelfId(),
        ~TProxy::EState(response->GetState()).ToString(),
        response->GetVoteId(),
        response->GetPriority(),
        ~epoch.ToString());

    if (response->GetState() != TProxy::EState::Leading)
        return;

    TGuard<TSpinLock> guard(SpinLock);    
    if (asyncResult->IsSet())
        return;

    YASSERT(voteId == response->GetSelfId());

    TResult result;
    result.Address = address;
    result.Id = voteId;
    result.Epoch = epoch;
    asyncResult->Set(result);

    awaiter->Cancel();

    LOG_INFO("Leader found (Address: %s, Id: %d, Epoch: %s)",
        ~address,
        response->GetSelfId(),
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
