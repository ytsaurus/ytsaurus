#include "public.h"

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellTracker
    : public TRefCounted
{
public:
    std::vector<NElection::TCellId> Select(const std::vector<NElection::TCellId>& candidates);

    void Update(const std::vector<NElection::TCellId>& toRemove, const std::vector<NElection::TCellId>& toAdd);
private:
    TSpinLock SpinLock_;
    THashSet<NElection::TCellId> CellIds_;
};

DEFINE_REFCOUNTED_TYPE(TCellTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
