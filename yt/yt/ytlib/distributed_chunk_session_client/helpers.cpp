#include "helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

TJournalQuorums ComputeDefaultJournalQuorums(int replicationFactor)
{
    THROW_ERROR_EXCEPTION_IF(
        replicationFactor < 1,
        "Replication factor must be positive, got %v",
        replicationFactor);

    int writeQuorum = replicationFactor / 2 + 1;
    int readQuorum = replicationFactor - writeQuorum + 1;
    return {
        .ReadQuorum = readQuorum,
        .WriteQuorum = writeQuorum,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
