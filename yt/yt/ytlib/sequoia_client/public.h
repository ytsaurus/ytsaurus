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
    (ChunkLocationDisposal)
    (ChunkConfirmation)
    (DeadChunkReplicaRemoval)
    (GroundUpdateQueueFlush)
    (ObjectDestruction)
);

DEFINE_ENUM(ESequoiaTable,
    (PathToNodeId)
    (NodeIdToPath)
    (ChunkReplicas)
    (LocationReplicas)
    (UnapprovedChunkReplicas)
    (ChildNode)
    (Transactions)
    (TransactionDescendants)
    (TransactionReplicas)
    (DependentTransactions)
    (NodeForks)
    (NodeSnapshots)
    (PathForks)
    (ChildForks)
    (ResponseKeeper)
    (ChunkRefreshQueue)
);

DEFINE_ENUM(EGroundUpdateQueue,
    ((Sequoia)            (0))
);

namespace NRecords {

struct TPathToNodeId;
struct TNodeIdToPath;
struct TChildNode;

struct TChunkReplicas;
struct TLocationReplicas;
struct TUnapprovedChunkReplicas;

struct TTransaction;
struct TTransactionDescendant;
struct TTransactionReplica;
struct TDependentTransaction;

struct TNodeFork;
struct TPathFork;
struct TChildFork;
struct TNodeSnapshot;

struct TSequoiaResponseKeeper;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

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

struct TSequoiaTransactionOptions;

////////////////////////////////////////////////////////////////////////////////

DEFINE_STRING_SERIALIZABLE_ENUM(EForkKind,
    ((Regular)      (0))
    ((Tombstone)    (1))
    ((Snapshot)     (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
