#include "record_helpers.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/sequoia_client/records/acls.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_nodes.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/doomed_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/response_keeper.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.record.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NApi::NNative;
using namespace NObjectClient;

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

namespace {

void MaybeAddTransactionIdTag(TStrongOrderingTags* strongOrderingTags, TTransactionId transactionId)
{
    if (transactionId == NullTransactionId) {
        return;
    }

    strongOrderingTags->UniversalTags.insert(ToString(transactionId));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Acl stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TAclsKey& /*key*/)
{
    return TStrongOrderingTags{.UniversalTags = {GroundUpdateQueueOrderingTag}};
}

// Resolve stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChildNodeKey& key)
{
    TStrongOrderingTags tags;
    tags.CellTagToTags = {{CellTagFromId(key.ParentId), {ToString(key.ParentId)}}};
    MaybeAddTransactionIdTag(&tags, key.TransactionId);
    return tags;
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeIdToPathKey& key)
{
    TStrongOrderingTags tags{.UniversalTags = {ToString(key.NodeId)}};
    MaybeAddTransactionIdTag(&tags, key.TransactionId);
    return tags;
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TPathToNodeIdKey& /*key*/)
{
    // PathToNodeId is a mirror for NodeIdToPath, it's safe to assume that
    // they are modified together. Let's not bother with the tags here.
    return {};
}

// Fork stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChildForkKey& key)
{
    TStrongOrderingTags tags;
    tags.CellTagToTags = {{CellTagFromId(key.ParentId), {ToString(key.ParentId)}}};
    MaybeAddTransactionIdTag(&tags, key.TransactionId);
    return tags;
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeForkKey& key)
{
    // This might be excessive. Think about it.
    TStrongOrderingTags tags{.UniversalTags = {ToString(key.NodeId)}};
    MaybeAddTransactionIdTag(&tags, key.TransactionId);
    return tags;
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeSnapshotKey& key)
{
    TStrongOrderingTags tags{.UniversalTags = {ToString(key.NodeId)}};
    MaybeAddTransactionIdTag(&tags, key.TransactionId);
    return tags;
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TPathForkKey& /*key*/)
{
    // Same as with the TPathToNodeId.
    return {};
}

// Transaction stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TDependentTransactionKey& key)
{
    return TStrongOrderingTags{.UniversalTags = {ToString(key.TransactionId), ToString(key.DependentTransactionId)}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TDoomedTransactionKey& key)
{
    return TStrongOrderingTags{.UniversalTags = {ToString(key.TransactionId)}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionDescendantKey& /*key*/)
{
    // This table is optimized for selects, and adding that many tags is
    // excessive. There is a somewhat bootleg solution to most reordering
    // related problems. For more see ValidateAndFinishTargetTransaction.
    return {};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionReplicaKey& key)
{
    return TStrongOrderingTags{.UniversalTags = {ToString(key.TransactionId)}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionKey& key)
{
    return TStrongOrderingTags{.UniversalTags = {ToString(key.TransactionId)}};
}

// Response keeper stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TSequoiaResponseKeeperKey& key)
{
    return TStrongOrderingTags{.UniversalTags = {ToString(key.MutationId)}};
}

// Chunk replica stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChunkRefreshQueue& /*record*/)
{
    return TStrongOrderingTags{.UniversalTags = {SequoiaReplicasOrderingTag}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChunkReplicasKey& /*key*/)
{
    return TStrongOrderingTags{.UniversalTags = {SequoiaReplicasOrderingTag}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TLocationReplicasKey& /*key*/)
{
    return TStrongOrderingTags{.UniversalTags = {SequoiaReplicasOrderingTag}};
}

TStrongOrderingTags GetStrongOrderingTags(const NRecords::TUnapprovedChunkReplicasKey& /*key*/)
{
    return TStrongOrderingTags{.UniversalTags = {SequoiaReplicasOrderingTag}};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
