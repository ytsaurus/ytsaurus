#include "downed_cell_tracker.h"
#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NHiveClient {

using namespace NElection;
using namespace NObjectClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TDownedCellTracker::TDownedCellTracker(const TDownedCellTrackerConfigPtr& config)
    : ChaosCellExpirationTime_(config->ChaosCellExpirationTime)
    , TabletCellExpirationTime_(config->TabletCellExpirationTime)
{ }

void TDownedCellTracker::Reconfigure(const TDownedCellTrackerConfigPtr& config)
{
    ChaosCellExpirationTime_ = config->ChaosCellExpirationTime;
    TabletCellExpirationTime_ = config->TabletCellExpirationTime;
}

std::vector<TCellId> TDownedCellTracker::RetainDowned(const std::vector<TCellId>& candidates, TInstant now)
{
    std::vector<TCellId> result;
    if (IsEmpty_.load()) {
        return result;
    }

    result.reserve(candidates.size());

    auto guard = Guard(SpinLock_);
    if (GuardedExpire(now, guard)) {
        return result;
    }

    for (const auto& id : candidates) {
        if (DownedCellIds_.find(id) != DownedCellIds_.end()) {
            result.push_back(id);
        }
    }

    return result;
}

TCellId TDownedCellTracker::ChooseOne(const std::vector<TCellId>& candidates, TInstant now)
{
    if (IsEmpty_.load()) {
        // Fast path.
        return candidates[RandomNumber(candidates.size())];
    }

    // Slow path.
    int candidatesCount = candidates.size();

    std::vector<TCellId> notBannedCandidates;
    notBannedCandidates.reserve(candidatesCount);

    auto firstToExpireCandidate = NullCellId;
    auto firstToExpireCandidateExpiration = TInstant::Max();

    {
        auto guard = Guard(SpinLock_);
        if (GuardedExpire(now, guard)) {
            guard.Release();
            return candidates[RandomNumber(static_cast<ui32>(candidatesCount))];
        }

        for (const auto& candidateCellId : candidates) {
            if (auto it = DownedCellIds_.find(candidateCellId); it == DownedCellIds_.end()) {
                notBannedCandidates.push_back(candidateCellId);
            } else if (notBannedCandidates.empty()) {
                auto expirationInstant = it->second->first;
                if (expirationInstant < firstToExpireCandidateExpiration) {
                    firstToExpireCandidate = candidateCellId;
                    firstToExpireCandidateExpiration = expirationInstant;
                }
            }
        }
    }

    if (notBannedCandidates.empty()) {
        return firstToExpireCandidate;
    }

    return notBannedCandidates[RandomNumber(notBannedCandidates.size())];
}

void TDownedCellTracker::Update(
    const std::vector<TCellId>& toRemove,
    const std::vector<TCellId>& toAdd,
    TInstant now)
{
    auto guard = Guard(SpinLock_);
    if (!toAdd.empty()) {
        IsEmpty_.store(false);
    }

    for (const auto& id : toRemove) {
        if (auto it = DownedCellIds_.find(id); it != DownedCellIds_.end()) {
            ExpirationList_.erase(it->second);
            DownedCellIds_.erase(it);
        }
    }

    for (const auto& id : toAdd) {
        if (auto [downedCellIdsIt, inserted] = DownedCellIds_.try_emplace(id, ExpirationList_.end()); inserted) {
            auto it = ExpirationList_.emplace(
                ExpirationList_.end(),
                GetExpirationTime(id, now),
                id);
            downedCellIdsIt->second = it;
        }
    }

    GuardedExpire(now, guard);
}

bool TDownedCellTracker::GuardedExpire(TInstant now, const TGuard<TSpinLock>& /*guard*/)
{
    YT_VERIFY(SpinLock_.IsLocked());

    if (IsEmpty_.load()) {
        return true;
    }

    while (!ExpirationList_.empty() && ExpirationList_.front().first <= now) {
        DownedCellIds_.erase(ExpirationList_.front().second);
        ExpirationList_.pop_front();
    }

    if (ExpirationList_.empty()) {
        IsEmpty_.store(true);
    }

    return IsEmpty_.load();
}

TInstant TDownedCellTracker::GetExpirationTime(TCellId cellId, TInstant now) const
{
    auto expirationDuration = TypeFromId(cellId) == EObjectType::ChaosCell
        ? ChaosCellExpirationTime_.load()
        : TabletCellExpirationTime_.load();

    return now + expirationDuration;
}

bool TDownedCellTracker::IsEmpty() const
{
    return IsEmpty_.load();
}

bool TDownedCellTracker::IsDowned(TCellId cellId) const
{
    auto guard = Guard(SpinLock_);
    return DownedCellIds_.contains(cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient

