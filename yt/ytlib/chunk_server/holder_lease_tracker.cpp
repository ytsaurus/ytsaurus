#include "stdafx.h"
#include "holder_lease_tracker.h"
#include "chunk_manager.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

THolderLeaseTracker::THolderLeaseTracker(
    const TConfig& config,
    TChunkManager* chunkManager,
    IInvoker* invoker)
    : Config(config)
    , ChunkManager(chunkManager)
    , Invoker(invoker)
{
    YASSERT(chunkManager != NULL);
    YASSERT(invoker != NULL);
}

void THolderLeaseTracker::RegisterHolder(const THolder& holder)
{
    YASSERT(~Invoker != NULL);

    auto pair = HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo()));
    YASSERT(pair.Second());

    auto& holderInfo = pair.First()->Second();
    holderInfo.Lease = TLeaseManager::CreateLease(
        Config.HolderLeaseTimeout,
        ~FromMethod(
            &THolderLeaseTracker::OnExpired,
            TPtr(this),
            holder.GetId())
        ->Via(Invoker));
}

void THolderLeaseTracker::UnregisterHolder(const THolder& holder)
{
    auto& holderInfo = GetHolderInfo(holder.GetId());
    TLeaseManager::CloseLease(holderInfo.Lease);
    YASSERT(HolderInfoMap.erase(holder.GetId()) == 1);
}

void THolderLeaseTracker::RenewHolderLease(const THolder& holder)
{
    YASSERT(~Invoker != NULL);
    auto& holderInfo = GetHolderInfo(holder.GetId());
    TLeaseManager::RenewLease(holderInfo.Lease);
}

void THolderLeaseTracker::OnExpired(THolderId holderId)
{
    // Check if the holder is still registered.
    auto* holderInfo = FindHolderInfo(holderId);
    if (holderInfo == NULL)
        return;

    LOG_INFO("Holder expired (HolderId: %d)", holderId);

    ChunkManager
        ->InitiateUnregisterHolder(holderId)
        ->Commit();
}

THolderLeaseTracker::THolderInfo* THolderLeaseTracker::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : &it->Second();
}

THolderLeaseTracker::THolderInfo& THolderLeaseTracker::GetHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    YASSERT(it != HolderInfoMap.end());
    return it->Second();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
