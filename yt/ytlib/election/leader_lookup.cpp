#include "leader_lookup.h"
#include "election_manager.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("LeaderLookup");

////////////////////////////////////////////////////////////////////////////////

TLeaderLookup::TLeaderLookup(
    const TConfig& config,
    TCellManager::TPtr cellManager)
    : Config(config)
    , CellManager(cellManager)
{}

TLeaderLookup::TLookupResult::TPtr TLeaderLookup::GetLeader()
{
    TLookupResult::TPtr asyncResult = new TLookupResult();
    TParallelAwaiter::TPtr awaiter = new TParallelAwaiter();

    for (TMasterId i = 0; i < CellManager->GetMasterCount(); ++i) {
        LOG_DEBUG("Requesting leader from master %d", i);

        TAutoPtr<TProxy> proxy = CellManager->GetMasterProxy<TProxy>(i);
        TProxy::TReqGetStatus::TPtr request = proxy->GetStatus();
        awaiter->Await(request->Invoke(Config.Timeout), FromMethod(
            &TLeaderLookup::OnResponse,
            awaiter, asyncResult, i));
    }
    
    awaiter->Complete(FromMethod(
        &TLeaderLookup::OnComplete,
        asyncResult));
    return asyncResult;
}

void TLeaderLookup::OnResponse(
    TProxy::TRspGetStatus::TPtr response,
    TParallelAwaiter::TPtr awaiter,
    TLookupResult::TPtr asyncResult,
    TMasterId masterId)
{
    if (!response->IsOK()) {
        LOG_WARNING("Error %s requesting leader from master %d",
            ~response->GetErrorCode().ToString(),
            masterId);
        return;
    }

    LOG_DEBUG("Reported status on master %d: "
        "state = %d, vote = %d, priority = %" PRIx64 ", epoch %s",
        masterId,
        response->GetState(),
        response->GetVoteId(),
        response->GetPriority(),
        ~TGuid::FromProto(response->GetVoteEpoch()).ToString());

    switch (response->GetState()) {
        case TProxy::EState::Leading:
            YASSERT(response->GetVoteId() == masterId);
            break;
        case TProxy::EState::Following:
            break;
        default:
            return;
    }
    
    TMasterId leaderId = response->GetVoteId();
    YASSERT(leaderId != InvalidMasterId);
    TGuid epoch = TGuid::FromProto(response->GetVoteEpoch());

    LOG_INFO("Obtained leader %d with epoch %s from master %d",
        leaderId, ~epoch.ToString(), masterId);

    TResult result;
    result.LeaderId = leaderId;
    result.Epoch = epoch;
    asyncResult->Set(result);

    awaiter->Cancel();
}

void TLeaderLookup::OnComplete(TLookupResult::TPtr asyncResult)
{
    LOG_INFO("No leader can be obtained from masters");
    TResult result;
    result.LeaderId = InvalidMasterId;
    result.Epoch = TGuid();
    asyncResult->Set(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
