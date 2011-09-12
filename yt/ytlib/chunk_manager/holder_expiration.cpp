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
    , LeaseManager(New<TLeaseManager>())
{ }

void THolderExpiration::Start(IInvoker::TPtr invoker)
{
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
    THolderInfo& holderInfo = pair.First()->Second();
    holderInfo.Lease = LeaseManager->CreateLease(
        Config.HolderLeaseTimeout,
        FromMethod(
            &THolderExpiration::OnExpired,
            TPtr(this),
            holder.Id)
        ->Via(Invoker));
}

void THolderExpiration::RemoveHolder(const THolder& holder)
{
    THolderInfo& holderInfo = GetHolderInfo(holder.Id);
    LeaseManager->CloseLease(holderInfo.Lease);
    YASSERT(HolderInfoMap.erase(holder.Id) == 1);
}

void THolderExpiration::RenewHolder(const THolder& holder)
{
    YASSERT(~Invoker != NULL);
    THolderInfo& holderInfo = GetHolderInfo(holder.Id);
    LeaseManager->RenewLease(holderInfo.Lease);
}

void THolderExpiration::OnExpired(THolderId holderId)
{
    // Check if the holder is still registered.
    THolderInfo* holderInfo = FindHolderInfo(holderId);
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
