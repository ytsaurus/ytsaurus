#include "tablet.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

void TTablet::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    NLsm::TTablet::Persist(context);

    Persist<TVectorSerializer<TUniquePtrSerializer<>>>(context, Partitions_);
    Persist<TUniquePtrSerializer<>>(context, Eden_);

    if (context.IsLoad()) {
        for (auto& partition : Partitions_) {
            partition->SetTablet(this);
        }
        Eden_->SetTablet(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
