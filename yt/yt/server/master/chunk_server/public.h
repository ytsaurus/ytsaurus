#pragma once

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/chunk_server/public.h>
#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <library/cpp/yt/containers/sharded_set.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <map>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::TChunkViewId;
using NChunkClient::TChunkListId;
using NChunkClient::TChunkTreeId;
using NChunkClient::TMediumId;
using NChunkClient::NullChunkId;
using NChunkClient::NullChunkListId;
using NChunkClient::NullChunkTreeId;
using NChunkClient::TBlockOffset;
using NChunkClient::EChunkType;
using NChunkClient::TBlockId;
using NChunkClient::TypicalReplicaCount;
using NChunkClient::MaxMediumCount;
using NChunkClient::MediumIndexBound;
using NChunkClient::DefaultStoreMediumIndex;
using NChunkClient::MaxMediumPriority;
using NChunkClient::TDataCenterName;
using NChunkClient::TChunkLocationUuid;
using NChunkClient::TMediumMap;
using NChunkClient::TCompactMediumMap;
using NChunkClient::TConsistentReplicaPlacementHash;
using NChunkClient::NullConsistentReplicaPlacementHash;
using NChunkClient::ChunkReplicaIndexBound;
using NChunkClient::TChunkReplicaWithLocationList;
using NChunkClient::ChunkShardCount;
using NChunkClient::TypicalChunkLocationCount;

using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;
using NNodeTrackerClient::MaxNodeId;

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NNodeTrackerServer::TNode;
using NNodeTrackerServer::TNodeList;

using NTabletClient::TDynamicStoreId;

using TChunkLocationId = NObjectClient::TObjectId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TRealChunkLocation, TChunkLocationId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TChunk, TChunkId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TChunkView, TChunkViewId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TDynamicStore, TDynamicStoreId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TChunkList, TChunkListId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TDomesticMedium, TMediumId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TS3Medium, TMediumId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TMedium, TMediumId, NObjectClient::TDirectObjectIdHash)

DECLARE_MASTER_OBJECT_TYPE(TRealChunkLocation)
DECLARE_MASTER_OBJECT_TYPE(TChunk)
DECLARE_MASTER_OBJECT_TYPE(TChunkList)
DECLARE_MASTER_OBJECT_TYPE(TChunkOwnerBase)
DECLARE_MASTER_OBJECT_TYPE(TDomesticMedium)
DECLARE_MASTER_OBJECT_TYPE(TS3Medium)

class TChunkLocation;
class TRealChunkLocation;
class TImaginaryChunkLocation;

using TChunkLocationList = TCompactVector<TChunkLocation*, TypicalReplicaCount>;

class TChunkTree;
class TChunkOwnerBase;

struct TChunkViewMergeResult;
class TChunkViewModifier;

class TChunkReplication;
class TChunkRequisition;
class TChunkRequisitionRegistry;

struct TChunkTreeStatistics;
struct TAggregatedNodeStatistics;

DECLARE_REFCOUNTED_CLASS(TJobTracker)

DECLARE_REFCOUNTED_CLASS(TJob)

DECLARE_REFCOUNTED_STRUCT(IChunkAutotomizer)
DECLARE_REFCOUNTED_STRUCT(IChunkManager)
DECLARE_REFCOUNTED_STRUCT(IChunkSealer)
DECLARE_REFCOUNTED_STRUCT(ICompositeJobController)
DECLARE_REFCOUNTED_STRUCT(IDataNodeTracker)
DECLARE_REFCOUNTED_STRUCT(IJobController)
DECLARE_REFCOUNTED_STRUCT(IJobRegistry)

DECLARE_REFCOUNTED_CLASS(TChunkMerger)
DECLARE_REFCOUNTED_STRUCT(IChunkReincarnator)
DECLARE_REFCOUNTED_CLASS(TChunkReplicator)
DECLARE_REFCOUNTED_CLASS(TChunkPlacement)
DECLARE_REFCOUNTED_CLASS(TConsistentChunkPlacement)
DECLARE_REFCOUNTED_STRUCT(IMasterCellChunkStatisticsCollector)
DECLARE_REFCOUNTED_STRUCT(IMasterCellChunkStatisticsPieceCollector)

DECLARE_REFCOUNTED_CLASS(TChunkManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicDataNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkTreeBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkAutotomizerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkMergerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicMasterCellChunkStatisticsCollectorConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkReincarnatorConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkManagerTestingConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChunkServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicAllyReplicaManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConsistentReplicaPlacementConfig)
DECLARE_REFCOUNTED_CLASS(TDomesticMediumConfig)
DECLARE_REFCOUNTED_CLASS(TS3MediumConfig)

//! Used as an expected upper bound in TCompactVector.
constexpr int TypicalChunkParentCount = 2;

//! The number of supported replication priorities.
//! The smaller the more urgent.
/*! current RF == 1 -> priority = 0
 *  current RF == 2 -> priority = 1
 *  current RF >= 3 -> priority = 2
 */
constexpr int ReplicationPriorityCount = 3;

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

DEFINE_ENUM(EChunkListKind,
    ((Static)                 (0))
    ((SortedDynamicRoot)      (1))
    ((SortedDynamicTablet)    (2))
    ((OrderedDynamicRoot)     (3))
    ((OrderedDynamicTablet)   (4))
    ((SortedDynamicSubtablet) (5))
    ((JournalRoot)            (6))
    ((HunkRoot)               (7))
    ((Hunk)                   (8))
    ((HunkStorageRoot)        (9))
    ((HunkTablet)            (10))
);

DEFINE_ENUM(EChunkListContentType,
    ((Main)                   (0))
    ((Hunk)                   (1))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EChunkReplicaState, i8,
    ((Generic)               (0))
    ((Active)                (1))
    ((Unsealed)              (2))
    ((Sealed)                (3))
);

DEFINE_ENUM(EChunkLocationState,
    // Belongs to a node that is not online.
    ((Offline) (0))
    // Belongs to a node that is online and reports presence of this location.
    ((Online)  (1))
    // Belongs to a node that is online but does not report presence of this location.
    ((Dangling)(2))
);

DEFINE_ENUM(EChunkTreeBalancerMode,
    // Strict is considered to be the default mode.
    ((Strict)           (0))
    // Permissive mode allows chunk tree to have higher rank,
    // more chunks per chunk list and higher chunks to chunk lists ratio.
    ((Permissive)       (1))
);

DEFINE_ENUM(EChunkDetachPolicy,
    ((SortedTablet)        (0))
    ((OrderedTabletPrefix) (1))
    ((OrderedTabletSuffix) (2))
    ((HunkTablet)          (3))
);

inline static const EChunkScanKind DelegatedScanKinds = EChunkScanKind::Refresh | EChunkScanKind::RequisitionUpdate;

using TFillFactorToNodeMap = std::multimap<double, NNodeTrackerServer::TNode*>;
using TFillFactorToNodeIterator = TFillFactorToNodeMap::iterator;

using TLoadFactorToNodeMap = std::multimap<double, NNodeTrackerServer::TNode*>;
using TLoadFactorToNodeIterator = TLoadFactorToNodeMap::iterator;

struct TChunkPartLossTimeComparer
{
    bool operator()(const TChunk* lhs, const TChunk* rhs) const;
};

using TOldestPartMissingChunkSet = std::set<TChunk*, TChunkPartLossTimeComparer>;

using TMediumSet = std::bitset<MaxMediumCount>;

constexpr int MediumDefaultPriority = 0;

using TChunkLists = TEnumIndexedArray<EChunkListContentType, TChunkList*>;

using TChunkRequisitionIndex = ui32;

using TJobEpoch = int;
constexpr TJobEpoch InvalidJobEpoch = -1;

using THeartbeatSequenceNumber = i64;
constexpr THeartbeatSequenceNumber InvalidHeartbeatSequenceNumber = -1;

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

struct TGlobalChunkScanDescriptor
{
    TChunk* FrontChunk;
    int ChunkCount;
    int ShardIndex;
};

struct TChunkToShardIndex
{
    int operator()(const TChunk* chunk) const;
};

using TShardedChunkSet = TShardedSet<TChunk*, ChunkShardCount, TChunkToShardIndex>;

//! Number of shards in sharded location map.
constexpr int ChunkLocationShardCount = 256;

//! A reasonable upper estimate on the number of cells 99% of chunks are exported to.
constexpr int TypicalChunkExportFactor = 4;

constexpr int MaxChunkCreationTimeHistogramBuckets = 50;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
