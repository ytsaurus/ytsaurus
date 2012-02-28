#include "stdafx.h"
#include "holder_lease_tracker.h"
#include "chunk_manager.h"

namespace NYT {
namespace NChunkServer {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

THolderLeaseTracker::THolderLeaseTracker(
    TConfig* config,
    TChunkManager* chunkManager,
    IInvoker* invoker)
    : Config(config)
    , ChunkManager(chunkManager)
    , Invoker(invoker)
{
	YASSERT(config);
    YASSERT(chunkManager);
    YASSERT(invoker);
}

void THolderLeaseTracker::OnHolderRegistered(const THolder& holder)
{
    auto pair = HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo()));
    YASSERT(pair.second);

    auto& holderInfo = pair.first->second;
    holderInfo.Lease = TLeaseManager::CreateLease(
        Config->HolderLeaseTimeout,
        ~FromMethod(
            &THolderLeaseTracker::OnExpired,
            TPtr(this),
            holder.GetId())
        ->Via(Invoker));
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
    ChunkManager
        ->InitiateUnregisterHolder(message)
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
