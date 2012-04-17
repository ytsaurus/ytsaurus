#include "stdafx.h"
#include "leader_lookup.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/ytree/ypath_client.h>

namespace NYT {
namespace NElection {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ElectionLogger;
static NProfiling::TProfiler Profiler("/election/leader_lookup");
static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TLeaderLookup::TLeaderLookup(TConfigPtr config)
    : Config(config)
{ }

TLeaderLookup::TAsyncResult TLeaderLookup::GetLeader()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto asyncResult = New<TFuture<TResult> >();
    auto awaiter = New<TParallelAwaiter>(&Profiler, "/time");

    FOREACH (Stroka address, Config->Addresses) {
        LOG_DEBUG("Requesting leader from peer %s", ~address);

        TProxy proxy(~ChannelCache.GetChannel(address));
        proxy.SetDefaultTimeout(Config->RpcTimeout);

        auto request = proxy.GetStatus();
        awaiter->Await(
            request->Invoke(),
            EscapeYPath(address),
            BIND(
                &TLeaderLookup::OnResponse,
                MakeStrong(this),
                awaiter,
                asyncResult,
                address));
    }
    
    awaiter->Complete(BIND(
        &TLeaderLookup::OnComplete,
        MakeStrong(this),
        asyncResult));

    return asyncResult;
}

void TLeaderLookup::OnResponse(
    TParallelAwaiter::TPtr awaiter,
    TFuture<TResult>::TPtr asyncResult,
    const Stroka& address,
    TProxy::TRspGetStatus::TPtr response)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_WARNING("Error requesting leader from peer %s\n%s",
            ~address,
            ~response->GetError().ToString());
        return;
    }

    auto voteId = response->vote_id();
    auto epoch = TEpoch::FromProto(response->vote_epoch());

    LOG_DEBUG("Received status from peer %s (PeerId: %d, State: %s, VoteId: %d, Priority: %" PRIx64 ", Epoch: %s)",
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

void TLeaderLookup::OnComplete(TPromise<TResult> promise)
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
