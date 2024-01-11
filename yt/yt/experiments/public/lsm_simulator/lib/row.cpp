#include "row.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

void TVersionedValue::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Value);
    Persist(context, Timestamp);
    Persist(context, DataSize);
}

void TRow::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Key);
    Persist(context, Values);
    Persist(context, DeleteTimestamps);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
