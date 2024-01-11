#include "store.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

void TStore::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    NLsm::TStore::Persist(context);

    Persist(context, Rows_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
