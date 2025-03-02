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
YT_DEFINE_STRONG_TYPEDEF(TRawYPath, TString);

template <bool Absolute>
class TBasicYPath;

template <bool Absolute>
class TBasicYPathBuf;

using TYPath = TBasicYPath<false>;
using TYPathBuf = TBasicYPathBuf<false>;

using TAbsoluteYPath = TBasicYPath<true>;
using TAbsoluteYPathBuf = TBasicYPathBuf<true>;

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaTransactionSequencingOptions;

////////////////////////////////////////////////////////////////////////////////

DEFINE_STRING_SERIALIZABLE_ENUM(EForkKind,
    ((Regular)      (0))
    ((Tombstone)    (1))
    ((Snapshot)     (2))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaReign,
    ((InitialReign) (1))
);

ESequoiaReign GetCurrentSequoiaReign() noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
