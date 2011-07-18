#include "chunk_manager.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

THolderTracker::THolderTracker(
    const TConfig& config,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , ServiceInvoker(serviceInvoker)
    , CurrentId(0)
{ }

THolder::TPtr THolderTracker::RegisterHolder(const THolderStatistics& statistics)
{
    int id = CurrentId++;
    
    THolder::TPtr holder = new THolder(id);
    
    holder->SetStatistics(statistics);

    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.HolderLeaseTimeout,
        FromMethod(
            &THolderTracker::OnHolderExpired,
            TPtr(this),
            holder)
        ->Via(ServiceInvoker));
    holder->SetLease(lease);

    // TODO: use YVERIFY
    VERIFY(Holders.insert(MakePair(id, holder)).Second(), "oops");

    LOG_INFO("Holder registered (Id: %d)", id);

    return holder;
}

THolder::TPtr THolderTracker::FindHolder(int id)
{
    THolderMap::iterator it = Holders.find(id);
    if (it == Holders.end())
        return NULL;
    
    THolder::TPtr holder = it->Second();
    RenewHolderLease(holder);
    return holder;
}

THolder::TPtr THolderTracker::GetHolder(int id)
{
    THolder::TPtr holder = FindHolder(id);
    if (~holder == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchHolderId) <<
            Sprintf("invalid or expired holder id %d", id);
    }
    return holder;
}

bool THolderTracker::IsHolderAlive(int id)
{
    return ~FindHolder(id) != NULL;
}

void THolderTracker::RenewHolderLease(THolder::TPtr holder)
{
    LeaseManager->RenewLease(holder->GetLease());
}

void THolderTracker::OnHolderExpired(THolder::TPtr holder)
{
    int id = holder->GetId();
    if (!IsHolderAlive(id))
        return;

    // TODO: use YVERIFY
    VERIFY(Holders.erase(id) == 1, "oops");

    LOG_INFO("Holder expired (Id: %d)", id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
