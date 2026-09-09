#pragma once

#include "public.h"

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/chunk_server/public.h>
#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/containers/sharded_set.h>

#include <set>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NChunkClient::NProto {

class TChunkSealInfo;
class TMiscExt;
class TChunkInfo;
class TChunkMeta;
class TChunkImportData;

class TReqCreateChunk;
class TRspCreateChunk;
class TReqConfirmChunk;
class TRspConfirmChunk;
class TReqSealChunk;
class TRspSealChunk;
class TReqCreateChunkLists;
class TRspCreateChunkLists;
class TReqUnstageChunkTree;
class TRspUnstageChunkTree;
class TReqAttachChunkTrees;
class TRspAttachChunkTrees;
class TReqDetachChunkTrees;
class TRspDetachChunkTrees;

} // namespace NYT::NChunkClient::NProto

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TBlockOffset;
using NChunkClient::TBlockId;

using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;
using NNodeTrackerClient::MaxNodeId;

////////////////////////////////////////////////////////////////////////////////

using TChunkLocationList = TCompactVector<TChunkLocation*, TypicalReplicaCount>;

struct TChunkViewMergeResult;

class TChunkRequisitionRegistry;

struct TAggregatedNodeStatistics;

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserContext)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkReplacerCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkStatisticsCalculatorCallbacks)

class TChunkStatisticsCalculator;

DECLARE_REFCOUNTED_CLASS(TJobTracker)

DECLARE_REFCOUNTED_CLASS(TJob)

DECLARE_REFCOUNTED_STRUCT(IChunkAutotomizer)
DECLARE_REFCOUNTED_STRUCT(IChunkSealer)
DECLARE_REFCOUNTED_STRUCT(ICompositeJobController)
DECLARE_REFCOUNTED_STRUCT(IJobController)
DECLARE_REFCOUNTED_CLASS(TChunkMerger)
DECLARE_REFCOUNTED_STRUCT(IChunkReincarnator)
DECLARE_REFCOUNTED_CLASS(TChunkReplicator)
DECLARE_REFCOUNTED_CLASS(TChunkPlacement)
DECLARE_REFCOUNTED_CLASS(TConsistentChunkPlacement)
DECLARE_REFCOUNTED_STRUCT(IMasterCellChunkStatisticsCollector)
DECLARE_REFCOUNTED_STRUCT(IMasterCellChunkStatisticsPieceCollector)

//! Used as an expected upper bound in TCompactVector.
constexpr int TypicalChunkParentCount = 2;

constexpr int DefaultConsistentReplicaPlacementReplicasPerChunk = 100;

DEFINE_BIT_ENUM(EChunkStatus,
    ((None)                            (0x0000))
    ((Underreplicated)                 (0x0001))
    ((Overreplicated)                  (0x0002))
    ((Lost)                            (0x0004))
    ((DataMissing)                     (0x0008))
    ((ParityMissing)                   (0x0010))
    ((UnexpectedOverreplicated)        (0x0020))
    ((Safe)                            (0x0040))
    ((TemporarilyUnavailable)          (0x0080))
    ((UnsafelyPlaced)                  (0x0100))
    ((DataDecommissioned)              (0x0200))
    ((ParityDecommissioned)            (0x0400))
    ((SealedMissing)                   (0x0800)) // Sealed chunk without sealed replicas (on certain medium).
    ((InconsistentlyPlaced)            (0x1000)) // For chunks with non-null consistent placement hash.
);

DEFINE_BIT_ENUM(ECrossMediumChunkStatus,
    ((None)              (0x0000))
    ((Sealed)            (0x0001))
    ((Lost)              (0x0004))
    ((DataMissing)       (0x0008))
    ((ParityMissing)     (0x0010))
    ((QuorumMissing)     (0x0020))
    ((Precarious)        (0x0200)) // All replicas are on transient media.
    ((MediumWiseLost)    (0x0400)) // Lost on some media, but not others.
    ((Deficient)         (0x0800)) // Underreplicated or {data,parity}-{missing,decommissioned} on some media.
);

DEFINE_BIT_ENUM(EChunkScanKind,
    ((None)                         (0x0000))
    ((Refresh)                      (0x0001))
    ((RequisitionUpdate)            (0x0002))
    ((Seal)                         (0x0004))
    ((Reincarnation)                (0x0008))
    ((GlobalStatisticsCollector)    (0x0010))
);

inline static const EChunkScanKind DelegatedScanKinds = EChunkScanKind::Refresh | EChunkScanKind::RequisitionUpdate;

using TFillFactorToNodeMap = std::multimap<double, NNodeTrackerServer::TNode*>;
using TFillFactorToNodeIterator = TFillFactorToNodeMap::iterator;

struct TChunkPartLossTimeComparer
{
    bool operator()(const TChunk* lhs, const TChunk* rhs) const;
};

using TOldestPartMissingChunkSet = std::set<TChunk*, TChunkPartLossTimeComparer>;

constexpr int MediumDefaultPriority = 0;

using TJobEpoch = int;
constexpr TJobEpoch InvalidJobEpoch = -1;

using TIncumbencyEpoch = int;
constexpr TIncumbencyEpoch InvalidIncumbencyEpoch = -1;
constexpr TIncumbencyEpoch NullIncumbencyEpoch = 0;

//! Refers to a requisition specifying that a chunk is not required by any account
//! on any medium.
constexpr TChunkRequisitionIndex EmptyChunkRequisitionIndex = 0;

//! Refers to a requisition specifying default RF on default medium under the
//! special migration account.
// NB: After we've migrated to chunk-wise accounting, that account and this
// index will be removed.
constexpr TChunkRequisitionIndex MigrationChunkRequisitionIndex = EmptyChunkRequisitionIndex + 1;

//! Refers to a requisition specifying RF of 2 on default medium under the
//! special migration account.
// NB: After we've migrated to chunk-wise accounting, that account and this
// index will be removed.
constexpr TChunkRequisitionIndex MigrationRF2ChunkRequisitionIndex = MigrationChunkRequisitionIndex + 1;

//! Refers to a requisition specifying RF of 1 on default medium under the special
//! migration account. Such requisition is suitable for erasure-coded chunks.
// NB: After we've migrated to chunk-wise accounting, that account and this
// index will be removed.
constexpr TChunkRequisitionIndex MigrationErasureChunkRequisitionIndex = MigrationRF2ChunkRequisitionIndex + 1;

constexpr i64 MaxReplicaLagLimit = Max<i64>() / 4;

struct TChunkToShardIndex
{
    int operator()(const TChunk* chunk) const;
};

using TShardedChunkSet = TShardedSet<TChunk*, ChunkShardCount, TChunkToShardIndex>;

//! A reasonable upper estimate on the number of cells 99% of chunks are exported to.
constexpr int TypicalChunkExportFactor = 4;

constexpr int MaxChunkCreationTimeHistogramBuckets = 50;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUpdateChunkPresence;
class TSequoiaReplicaInfo;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ChunkServerLogger, "ChunkServer");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ChunkServerProfiler, "/chunk_server");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ChunkServerHistogramProfiler, "/chunk_server/histograms");

YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ChunkServiceProfiler, "/chunk_service");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDataNodeTrackerInternal)
DECLARE_REFCOUNTED_STRUCT(IChunkReplicaFetcher)

DECLARE_REFCOUNTED_CLASS(TJobRegistry)

DECLARE_REFCOUNTED_STRUCT(ISequoiaReplicasModifier)
DECLARE_REFCOUNTED_STRUCT(ISequoiaChunkRefresher)
struct TSequoiaChunkRefresherStatus;

template <class TPayload>
class TChunkScanQueueWithPayload;
using TChunkScanQueue = TChunkScanQueueWithPayload<void>;

template <class TPayload>
class TChunkScannerWithPayload;
using TChunkScanner = TChunkScannerWithPayload<void>;

DEFINE_ENUM(EAddReplicaReason,
    (IncrementalHeartbeat)
    (FullHeartbeat)
    (Confirmation)
);

DEFINE_ENUM(ERemoveReplicaReason,
    (None)
    (IncrementalHeartbeat)
    (ApproveTimeout)
    (ChunkDestroyed)
    (NodeDisposed)
    (NodeRestarted)
    (SequoiaModified)
    (SequoiaNodeDisposed)
);

DEFINE_ENUM(ESequoiaReplicaModificationPhase,
    (StartTransaction)
    (GatherModifiedAddedReplicas)
    (ParseRemovedReplicas)
    (LookupRemovedLocationReplicas)
    (GatherModifiedRemovedReplicas)
    (WriteRowsAndAddTransactionActions)
    (CommitTransaction)
    (LookupExistingReplicasInReplacedLocation)
    (GatherReplacedLocationReplicasDifference)
    (SemaphoreWait)
);

constexpr int MinVitalReplicationFactor = 3;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
