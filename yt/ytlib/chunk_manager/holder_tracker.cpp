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

THolder::TPtr THolderTracker::RegisterHolder(
    const THolderStatistics& statistics,
    Stroka address)
{
    int id = CurrentId++;
    
    THolder::TPtr holder = new THolder(id, address);
    
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

    UpdateHolderPreference(holder);

    LOG_INFO("Holder registered (Id: %d, Address: %s)",
        id,
        ~address);

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
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
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

yvector<THolder::TPtr> THolderTracker::GetTargetHolders(int count)
{
    yvector<THolder::TPtr> result;
    THolder::TPreferenceMap::reverse_iterator it = PreferenceMap.rend();
    while (it != PreferenceMap.rbegin() && result.ysize() < count) {
        result.push_back((*it++).second);
    }
    return result;
}

void THolderTracker::UpdateHolderPreference(THolder::TPtr holder)
{
    if (holder->GetPreferenceIterator() != PreferenceMap.end()) {
        PreferenceMap.erase(holder->GetPreferenceIterator());
    }

    double preference = holder->GetPreference();
    THolder::TPreferenceMap::iterator it = PreferenceMap.insert(MakePair(preference, holder));
    holder->SetPreferenceIterator(it);
}

void THolderTracker::OnHolderExpired(THolder::TPtr holder)
{
    int id = holder->GetId();
    if (!IsHolderAlive(id))
        return;

    // TODO: use YVERIFY
    VERIFY(Holders.erase(id) == 1, "oops");

    if (holder->GetPreferenceIterator() != PreferenceMap.end()) {
        PreferenceMap.erase(holder->GetPreferenceIterator());
        holder->SetPreferenceIterator(PreferenceMap.end());
    }

    LOG_INFO("Holder expired (Id: %d)", id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
