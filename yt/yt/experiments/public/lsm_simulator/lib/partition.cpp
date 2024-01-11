#include "partition.h"
#include "structured_logger.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

void TPartition::SetState(EPartitionState state)
{
    State_ = state;
    if (GetTablet()->GetStructuredLogger()) {
        GetTablet()->GetStructuredLogger()->OnPartitionStateChanged(this);
    }
}

void TPartition::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    NLsm::TPartition::Persist(context);

    Persist<TVectorSerializer<TUniquePtrSerializer<>>>(context, Stores_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
