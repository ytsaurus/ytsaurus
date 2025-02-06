#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TDownedCellTracker
    : public TRefCounted
{
public:
    explicit TDownedCellTracker(const TDownedCellTrackerConfigPtr& config);

    std::vector<NElection::TCellId> RetainDowned(
        const std::vector<NElection::TCellId>& candidates,
        TInstant now = TInstant::Now());

    NElection::TCellId ChooseOne(
        const std::vector<NElection::TCellId>& candidates,
        TInstant now = TInstant::Now());

    void Update(
        const std::vector<NElection::TCellId>& toRemove,
        const std::vector<NElection::TCellId>& toAdd,
        TInstant now = TInstant::Now());

    void Reconfigure(const TDownedCellTrackerConfigPtr& config);

    bool IsEmpty() const;
    bool IsDowned(NElection::TCellId cellId) const;
private:
    using TExpirationListType = std::list<std::pair<TInstant, NElection::TCellId>>;

    std::atomic<TDuration> ChaosCellExpirationTime_;
    std::atomic<TDuration> TabletCellExpirationTime_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<NElection::TCellId, TExpirationListType::iterator> DownedCellIds_;
    TExpirationListType ExpirationList_;
    std::atomic<bool> IsEmpty_ = true;

    bool GuardedExpire(TInstant now, const TGuard<NThreading::TSpinLock>& /*guard*/);
    TInstant GetExpirationTime(NElection::TCellId cellId, TInstant now) const;
};

DEFINE_REFCOUNTED_TYPE(TDownedCellTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
