#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellTracker
    : public TRefCounted
{
public:
    std::vector<NElection::TCellId> Select(const std::vector<NElection::TCellId>& candidates);

    void Update(const std::vector<NElection::TCellId>& toRemove, const std::vector<NElection::TCellId>& toAdd);
private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashSet<NElection::TCellId> CellIds_;
};

DEFINE_REFCOUNTED_TYPE(TCellTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
