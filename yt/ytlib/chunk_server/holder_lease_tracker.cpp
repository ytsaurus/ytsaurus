#include "stdafx.h"
#include "holder_lease_tracker.h"
#include "chunk_manager.h"

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>

namespace NYT {
namespace NChunkServer {

using namespace NProto;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkServer");

////////////////////////////////////////////////////////////////////////////////

THolderLeaseTracker::THolderLeaseTracker(
    TConfig* config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void THolderLeaseTracker::OnHolderRegistered(const THolder& holder)
{
    auto pair = HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo()));
    YASSERT(pair.second);

    auto epochStateInvoker = Bootstrap
        ->GetMetaStateManager()
        ->GetEpochContext()
        ->CreateInvoker(Bootstrap->GetStateInvoker());

    auto& holderInfo = pair.first->second;
    holderInfo.Lease = TLeaseManager::CreateLease(
        Config->HolderLeaseTimeout,
        ~FromMethod(
            &THolderLeaseTracker::OnExpired,
            TPtr(this),
            holder.GetId())
        ->Via(epochStateInvoker));
}

void THolderLeaseTracker::OnHolderUnregistered(const THolder& holder)
{
    auto holderId = holder.GetId();
    auto& holderInfo = GetHolderInfo(holderId);
    TLeaseManager::CloseLease(holderInfo.Lease);
    YASSERT(HolderInfoMap.erase(holderId) == 1);
}

void THolderLeaseTracker::RenewHolderLease(const THolder& holder)
{
    auto& holderInfo = GetHolderInfo(holder.GetId());
    TLeaseManager::RenewLease(holderInfo.Lease);
}

void THolderLeaseTracker::OnExpired(THolderId holderId)
{
    // Check if the holder is still registered.
    auto* holderInfo = FindHolderInfo(holderId);
    if (!holderInfo)
        return;

    LOG_INFO("Holder expired (HolderId: %d)", holderId);


    TMsgUnregisterHolder message;
    message.set_holder_id(holderId);
    Bootstrap
        ->GetChunkManager()
        ->InitiateUnregisterHolder(message)
        ->SetRetriable(Config->HolderLeaseTimeout)
        ->OnSuccess(~FromFunctor([=] (TVoid)
            {
                LOG_INFO("Holder expiration commit success (HolderId: %d)", holderId);
            }))
        ->OnError(~FromFunctor([=] ()
            {
                LOG_INFO("Holder expiration commit failed (HolderId: %d)", holderId);
            }))
        ->Commit();
}

THolderLeaseTracker::THolderInfo* THolderLeaseTracker::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : &it->second;
}

THolderLeaseTracker::THolderInfo& THolderLeaseTracker::GetHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    YASSERT(it != HolderInfoMap.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
