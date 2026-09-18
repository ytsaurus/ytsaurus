#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <bitset>
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
using NChunkClient::EChunkType;
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
using NChunkClient::EChunkListContentType;
using NChunkClient::EChunkListKind;
using NChunkClient::EChunkReplicaState;

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NNodeTrackerServer::TNode;
using TNodeList = TCompactVector<TNode*, TypicalReplicaCount>;

using NTabletClient::TDynamicStoreId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TChunkLocation, NObjectClient::TObjectId, ::THash<NObjectClient::TObjectId>)
DECLARE_ENTITY_TYPE(TChunk, TChunkId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TChunkView, TChunkViewId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TDynamicStore, TDynamicStoreId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TChunkList, TChunkListId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TDomesticMedium, TMediumId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TS3Medium, TMediumId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TMedium, TMediumId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TChunkLocation)
DECLARE_MASTER_OBJECT_TYPE(TChunk)
DECLARE_MASTER_OBJECT_TYPE(TMainTreeChunkList)
DECLARE_MASTER_OBJECT_TYPE(THunkTreeChunkList)
DECLARE_MASTER_OBJECT_TYPE(TChunkList)
DECLARE_MASTER_OBJECT_TYPE(TChunkTree)
DECLARE_MASTER_OBJECT_TYPE(TChunkOwnerBase)
DECLARE_MASTER_OBJECT_TYPE(TMedium)
DECLARE_MASTER_OBJECT_TYPE(TDomesticMedium)
DECLARE_MASTER_OBJECT_TYPE(TS3Medium)
DECLARE_MASTER_OBJECT_TYPE(TDynamicStore)

class TChunkViewModifier;

class TChunkReplication;
class TChunkRequisition;

struct TChunkTreeStatistics;
struct THunkChunkTreeStatistics;

struct TChunkOwnerDataStatistics;

DECLARE_REFCOUNTED_STRUCT(IChunkManager)
DECLARE_REFCOUNTED_STRUCT(IDataNodeTracker)
DECLARE_REFCOUNTED_STRUCT(IJobRegistry)

DECLARE_REFCOUNTED_STRUCT(TChunkManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDanglingLocationCleanerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicDataNodeTrackerTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicDataNodeTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkTreeBalancerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkAutotomizerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkMergerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicMasterCellChunkStatisticsCollectorConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkReincarnatorConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkManagerTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicDataCenterFaultThresholdsConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicDataCenterFailureDetectorConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChunkServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicAllyReplicaManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicConsistentReplicaPlacementConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicSequoiaChunkReplicasConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicSequoiaChunkReplicasStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TDomesticMediumConfig)
DECLARE_REFCOUNTED_STRUCT(TS3MediumConfig)

//! The number of supported replication priorities.
//! The smaller the more urgent.
/*! current RF == 1 -> priority = 0
 *  current RF == 2 -> priority = 1
 *  current RF >= 3 -> priority = 2
 */
constexpr int ReplicationPriorityCount = 3;

//! Number of supported repair priorities. The smaller the more urgent.
/*! Parts we can still lose == 0 -> priority = 0
 *  Parts we can still lose == 1 -> priority = 1
 *  Parts we can still lose >= 2 -> priority = 2
 *  Decommissioned chunks -> priority = 3
 */
constexpr int RepairPriorityCount = 4;

DEFINE_ENUM(EChunkLocationState,
    // Belongs to a node that is not online.
    ((Offline)   (0))
    // Belongs to a node that has alive local state and reports presence of this location.
    ((Online)    (1))
    // Belongs to a node that is online but does not report presence of this location.
    ((Dangling)  (2))
    // The location is disposed. Can belong to any node, should be treated same way as Dangling.
    ((Disposed)  (3))
    // Belongs to a node that is registered and no heartbeat was sent for this location.
    ((Registered)(4))
    // Belongs to a node that has restarted, but did not report location in heartbeats.
    ((Restarted) (5))
);

DEFINE_ENUM(EChunkTreeBalancerMode,
    // Strict is considered to be the default mode.
    ((Strict)           (0))
    // Permissive mode allows chunk tree to have higher rank,
    // more chunks per chunk list and higher chunks to chunk lists ratio.
    ((Permissive)       (1))
);

DEFINE_ENUM(EChunkDetachPolicy,
    // For regular and hunk chunks of sorted tablets.
    ((SortedTablet)        (0))
    // For regular chunks of ordered tablets.
    ((OrderedTabletPrefix) (1))
    ((OrderedTabletSuffix) (2))
    // For chunks of hunk storage tablets.
    ((HunkTablet)          (3))
    // For hunk chunks of ordered tablets.
    ((OrderedTabletHunk)   (4))
    // For arbitrary chunks of scratch chunk lists.
    ((Scratch)             (5))
);

DEFINE_ENUM(EChunkMergerStatus,
    ((NotInMergePipeline)           (0))
    ((AwaitingMerge)                (1))
    ((InMergePipeline)              (2))
);

using TLoadFactorToNodeMap = std::multimap<double, NNodeTrackerServer::TNode*>;
using TLoadFactorToNodeIterator = TLoadFactorToNodeMap::iterator;

using TMediumSet = std::bitset<MaxMediumCount>;

using TChunkLists = TEnumIndexedArray<EChunkListContentType, TChunkList*>;

using TChunkRequisitionIndex = ui32;

using THeartbeatSequenceNumber = i64;
constexpr THeartbeatSequenceNumber InvalidHeartbeatSequenceNumber = -1;

struct TGlobalChunkScanDescriptor
{
    TChunk* FrontChunk;
    int ChunkCount;
    int ShardIndex;
};

//! Number of shards in sharded location map.
constexpr int ChunkLocationShardCount = 256;

// Only used for producing text representation of table chunk formats in
// deprecated TableChunkFormat and TableChunkFormatStatistics attributes.
// Keep in sync with NChunkClient::EChunkFormat.
TStringBuf SerializeChunkFormatAsTableChunkFormat(NChunkClient::EChunkFormat chunkFormat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
