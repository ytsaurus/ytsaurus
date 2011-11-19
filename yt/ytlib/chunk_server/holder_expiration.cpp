#include "stdafx.h"
#include "holder_expiration.h"
#include "chunk_manager.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

THolderExpiration::THolderExpiration(
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

void THolderExpiration::RegisterHolder(const THolder& holder)
{
    YASSERT(~Invoker != NULL);

    auto pair = HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo()));
    YASSERT(pair.Second());

    auto& holderInfo = pair.First()->Second();
    holderInfo.Lease = TLeaseManager::Get()->CreateLease(
        Config.HolderLeaseTimeout,
        FromMethod(
            &THolderExpiration::OnExpired,
            TPtr(this),
            holder.GetId())
        ->Via(Invoker));
}

void THolderExpiration::UnregisterHolder(const THolder& holder)
{
    auto& holderInfo = GetHolderInfo(holder.GetId());
    TLeaseManager::Get()->CloseLease(holderInfo.Lease);
    YASSERT(HolderInfoMap.erase(holder.GetId()) == 1);
}

void THolderExpiration::RenewHolderLease(const THolder& holder)
{
    YASSERT(~Invoker != NULL);
    auto& holderInfo = GetHolderInfo(holder.GetId());
    TLeaseManager::Get()->RenewLease(holderInfo.Lease);
}

void THolderExpiration::OnExpired(THolderId holderId)
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

THolderExpiration::THolderInfo* THolderExpiration::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : &it->Second();
}

THolderExpiration::THolderInfo& THolderExpiration::GetHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    YASSERT(it != HolderInfoMap.end());
    return it->Second();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
