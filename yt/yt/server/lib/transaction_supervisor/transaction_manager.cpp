#include "transaction_manager.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

void TTransactionCommitOptions::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CommitTimestamp);
    Persist(context, CommitTimestampClusterTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
