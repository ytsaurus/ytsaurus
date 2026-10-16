#pragma once

#include "public.h"

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TJournalQuorums
{
    int ReadQuorum = 0;
    int WriteQuorum = 0;
};

//! Derives safe read/write quorums for a replicated (non-erasure) journal: the
//! write quorum is the majority and the read quorum is the smallest value that
//! still intersects it (ReadQuorum + WriteQuorum == ReplicationFactor + 1). Yields
//! 1/2 for ReplicationFactor 2 and 2/2 for 3.
TJournalQuorums ComputeDefaultJournalQuorums(int replicationFactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
