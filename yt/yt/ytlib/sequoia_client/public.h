#pragma once

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/sequoia_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTransactionType,
    (CypressModification)
    (CypressTransactionMirroring)
    (ResponseKeeper)
    (IncrementalHeartbeat)
    (FullHeartbeat)
    (FinalizeHeartbeatSession)
    (ChunkLocationDisposal)
    (ChunkConfirmation)
    (DeadChunkReplicaRemoval)
    (GroundUpdateQueueFlush)
    (ObjectDestruction)
    (GlobalRefresh)
    (LocationRefresh)
);

DEFINE_ENUM(ESequoiaTable,
    (PathToNodeId)
    (NodeIdToPath)
    (ChunkReplicas)
    (LocationReplicas)
    (UnapprovedChunkReplicas)
    (ChildNodes)
    (Transactions)
    (TransactionDescendants)
    (TransactionReplicas)
    (DependentTransactions)
    (DoomedTransactions)
    (NodeForks)
    (NodeSnapshots)
    (PathForks)
    (ChildForks)
    (ResponseKeeper)
    (ChunkRefreshQueue)
    (Acls)
);

DEFINE_ENUM(EGroundUpdateQueue,
    ((Sequoia)            (0))
);

////////////////////////////////////////////////////////////////////////////////

namespace NRecords {

#define FOR_EACH_RECORD(record) \
    record(Acls) \
    record(PathToNodeId) \
    record(NodeIdToPath) \
    record(ChildNode) \
    record(ChunkReplicas) \
    record(LocationReplicas) \
    record(UnapprovedChunkReplicas) \
    record(Transaction) \
    record(TransactionDescendant) \
    record(TransactionReplica) \
    record(DependentTransaction) \
    record(DoomedTransaction) \
    record(NodeFork) \
    record(PathFork) \
    record(ChildFork) \
    record(NodeSnapshot) \
    record(SequoiaResponseKeeper)

#define DECLARE_RECORD_AND_KEY(recordName) \
    struct T##recordName; \
    struct T##recordName##Key;

FOR_EACH_RECORD(DECLARE_RECORD_AND_KEY)
#undef DECLARE_RECORD_AND_KEY
#undef FOR_EACH_RECORD

// Chunk refresh queue has no key.
struct TChunkRefreshQueue;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaConnection)
DECLARE_REFCOUNTED_STRUCT(ISequoiaClient)
DECLARE_REFCOUNTED_STRUCT(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TMangledSequoiaPath, TString);
//! A canonical absolute path. The canonical path format restricts the YPath grammar,
//! allowing only a sequence of segments without additional YPath features.
YT_DEFINE_STRONG_TYPEDEF(TRealPath, TString);

YT_DEFINE_STRONG_TYPEDEF(TRawYPath, TString);

//! The following classes are the wrappers for the canonical path representation.
template <bool Absolute>
class TBasicPathBuf;

class TRelativePath;
using TRelativePathBuf = TBasicPathBuf<false>;

//! An absolute path is a canonical path that starts with a slash root designator.
class TAbsolutePath;
using TAbsolutePathBuf = TBasicPathBuf<true>;

////////////////////////////////////////////////////////////////////////////////

struct TPrerequisiteRevision;
struct TResolvedPrerequisiteRevision;

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaTransactionFeatures;
struct TSequoiaTransactionOptions;

////////////////////////////////////////////////////////////////////////////////

DEFINE_STRING_SERIALIZABLE_ENUM(EForkKind,
    ((Regular)      (0))
    ((Tombstone)    (1))
    ((Snapshot)     (2))
);

////////////////////////////////////////////////////////////////////////////////

struct TStrongOrderingTags
{
    THashSet<std::string> UniversalTags;
    THashMap<NObjectClient::TCellTag, THashSet<std::string>> CellTagToTags;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
