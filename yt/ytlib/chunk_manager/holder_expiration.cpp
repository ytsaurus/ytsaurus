#include "../misc/stdafx.h"
#include "holder_expiration.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

THolderExpiration::THolderExpiration(
    const TConfig& config,
    TChunkManager::TPtr chunkManager)
    : Config(config)
    , ChunkManager(chunkManager)
{
    YASSERT(~chunkManager != NULL);
}

void THolderExpiration::Start(IInvoker::TPtr invoker)
{
    YASSERT(~invoker != NULL);

    YASSERT(~Invoker == NULL);
    Invoker = invoker;
}

void THolderExpiration::Stop()
{
    YASSERT(~Invoker != NULL);
    Invoker.Drop();
}

void THolderExpiration::AddHolder(const THolder& holder)
{
    YASSERT(~Invoker != NULL);
    auto pair = HolderInfoMap.insert(MakePair(holder.Id, THolderInfo()));
    YASSERT(pair.Second());
    auto& holderInfo = pair.First()->Second();
    holderInfo.Lease = TLeaseManager::Get()->CreateLease(
        Config.HolderLeaseTimeout,
        FromMethod(
            &THolderExpiration::OnExpired,
            TPtr(this),
            holder.Id)
        ->Via(Invoker));
}

void THolderExpiration::RemoveHolder(const THolder& holder)
{
    auto& holderInfo = GetHolderInfo(holder.Id);
    TLeaseManager::Get()->CloseLease(holderInfo.Lease);
    YASSERT(HolderInfoMap.erase(holder.Id) == 1);
}

void THolderExpiration::RenewHolder(const THolder& holder)
{
    YASSERT(~Invoker != NULL);
    auto& holderInfo = GetHolderInfo(holder.Id);
    TLeaseManager::Get()->RenewLease(holderInfo.Lease);
}

void THolderExpiration::OnExpired(THolderId holderId)
{
    // Check if the holder is still registered.
    auto* holderInfo = FindHolderInfo(holderId);
    if (holderInfo == NULL)
        return;

    LOG_INFO("Holder expired (HolderId: %d)", holderId);

    ChunkManager->UnregisterHolder(holderId);
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

} // namespace NChunkManager
} // namespace NYT
