#include "record_helpers.h"

#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

bool IsTombstone(const NRecords::TPathToNodeId& record)
{
    return record.NodeId == NodeTombstoneId;
}

bool IsTombstone(const NRecords::TPathFork& record)
{
    return record.NodeId == NodeTombstoneId;
}

bool IsTombstone(const NRecords::TNodeIdToPath& record)
{
    return record.ForkKind == EForkKind::Tombstone;
}

bool IsTombstone(const NRecords::TNodeFork& record)
{
    // Every ID can occur in "node_forks" table at most twice: one creation
    // and on removal. This allows us to derive fork kind from
    // "progenitor_transaction_id" field.

    // NB: The alternative solution could be to mark such tombstone records
    // with null path.

    return record.ProgenitorTransactionId != record.Key.TransactionId;
}

bool IsTombstone(const NRecords::TChildNode& record)
{
    return record.ChildId == ChildNodeTombstoneId;
}

bool IsTombstone(const NRecords::TChildFork& record)
{
    return record.ChildId == ChildNodeTombstoneId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
