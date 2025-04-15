#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/server/lib/chunk_server/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::TSessionId;
using NChunkClient::ESessionType;
using NChunkClient::TBlockId;
using NChunkClient::TChunkLocationUuid;

using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

struct TChunkDescriptor;
struct TSessionOptions;
struct TChunkReadOptions;

class TChunkReadGuard;

struct TArtifactKey;

class TNetworkStatistics;

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

DECLARE_REFCOUNTED_CLASS(TMasterJobBase)
DECLARE_REFCOUNTED_STRUCT(IJobController)

DECLARE_REFCOUNTED_STRUCT(IChunkStoreHost)
DECLARE_REFCOUNTED_CLASS(TChunkStore)

DECLARE_REFCOUNTED_STRUCT(IAllyReplicaManager)
DECLARE_REFCOUNTED_STRUCT(IChunkRegistry)

DECLARE_REFCOUNTED_CLASS(TChunkReaderSweeper)

DECLARE_REFCOUNTED_STRUCT(IBlobReaderCache)

DECLARE_REFCOUNTED_STRUCT(IJournalDispatcher)
DECLARE_REFCOUNTED_STRUCT(IJournalManager)

DECLARE_REFCOUNTED_CLASS(TCachedChunkMeta)
DECLARE_REFCOUNTED_CLASS(TCachedBlocksExt)
DECLARE_REFCOUNTED_STRUCT(IChunkMetaManager)

DECLARE_REFCOUNTED_CLASS(TChunkLocation)
DECLARE_REFCOUNTED_CLASS(TStoreLocation)
DECLARE_REFCOUNTED_STRUCT(TLocationPerformanceCounters)

DECLARE_REFCOUNTED_STRUCT(TChunkContext)
DECLARE_REFCOUNTED_STRUCT(IChunk)
DECLARE_REFCOUNTED_CLASS(TBlobChunkBase)
DECLARE_REFCOUNTED_CLASS(TStoredBlobChunk)
DECLARE_REFCOUNTED_CLASS(TJournalChunk)

DECLARE_REFCOUNTED_CLASS(TLocationManager)
DECLARE_REFCOUNTED_CLASS(TLocationHealthChecker)

DECLARE_REFCOUNTED_STRUCT(ISession)
DECLARE_REFCOUNTED_STRUCT(TNbdSession)
DECLARE_REFCOUNTED_CLASS(TBlobWritePipeline)
DECLARE_REFCOUNTED_CLASS(TBlobSession)
DECLARE_REFCOUNTED_CLASS(TSessionManager)

DECLARE_REFCOUNTED_STRUCT(TChunkLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TChunkLocationDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TStoreLocationDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TCacheLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiplexedChangelogConfig)
DECLARE_REFCOUNTED_STRUCT(TArtifactCacheReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TRepairReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TSealReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TAllyReplicaManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TDataNodeTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TJournalManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TJobControllerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TDataNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TDataNodeDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TLayerLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TTmpfsLayerCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TVolumeManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TTableSchemaCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TTableSchemaCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TRepairReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TMediumThroughputMeterConfig)
DECLARE_REFCOUNTED_STRUCT(TIOThroughputMeterConfig)
DECLARE_REFCOUNTED_STRUCT(TLocationHealthCheckerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TRemoveChunkJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicateChunkJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMergeWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TMergeChunksJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TRepairReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TRepairChunkJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TAutotomizeChunkJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TReincarnateReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TReincarnateWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TReincarnateChunkJobDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSealChunkJobDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TTableSchemaCache)
DECLARE_REFCOUNTED_CLASS(TCachedTableSchemaWrapper)
DECLARE_REFCOUNTED_STRUCT(IOffloadedChunkReadSession)

DECLARE_REFCOUNTED_CLASS(TMediumDirectoryManager)
DECLARE_REFCOUNTED_CLASS(TMediumUpdater)

DECLARE_REFCOUNTED_CLASS(TP2PBlockCache)
DECLARE_REFCOUNTED_CLASS(TP2PSnooper)
DECLARE_REFCOUNTED_CLASS(TP2PDistributor)
DECLARE_REFCOUNTED_STRUCT(TP2PConfig)
DECLARE_REFCOUNTED_STRUCT(TP2PChunkPeer)
DECLARE_REFCOUNTED_STRUCT(TP2PBlock)
DECLARE_REFCOUNTED_STRUCT(TP2PChunk)
DECLARE_REFCOUNTED_STRUCT(IIOThroughputMeter)

DECLARE_REFCOUNTED_STRUCT(ILocalChunkFragmentReader)

DECLARE_REFCOUNTED_STRUCT(INbdChunkHandler)

DECLARE_REFCOUNTED_CLASS(TProbePutBlocksRequestSupplier)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((LocalChunkReaderFailed)    (1300))
    // This is deprecated, since volume manager is now a part of data node.
    // ((LayerUnpackingFailed)      (1301))
);

DEFINE_ENUM(EDataNodeThrottlerKind,
    //! Controls the total incoming bandwidth.
    (TotalIn)
    //! Controls the total outcoming bandwidth.
    (TotalOut)
    //! Controls incoming bandwidth used by replication jobs.
    (ReplicationIn)
    //! Controls outcoming bandwidth used by replication jobs.
    (ReplicationOut)
    //! Controls incoming bandwidth used by repair jobs.
    (RepairIn)
    //! Controls outcoming bandwidth used by repair jobs.
    (RepairOut)
    //! Controls incoming bandwidth used by merge jobs.
    (MergeIn)
    //! Controls outcoming bandwidth used by merge jobs.
    (MergeOut)
    //! Controls incoming bandwidth used by autotomy jobs.
    (AutotomyIn)
    //! Controls outcoming bandwidth used by autotomy jobs.
    (AutotomyOut)
    //! Controls incoming bandwidth used by Artifact Cache downloads.
    (ArtifactCacheIn)
    //! Controls outcoming bandwidth used by Artifact Cache downloads.
    (ArtifactCacheOut)
    //! Controls outcoming bandwidth used by Skynet sharing.
    (SkynetOut)
    //! Controls incoming bandwidth used by tablet compaction and partitioning.
    (TabletCompactionAndPartitioningIn)
    //! Controls outcoming bandwidth used by tablet compaction and partitioning.
    (TabletCompactionAndPartitioningOut)
    //! Controls incoming bandwidth used by tablet journals.
    (TabletLoggingIn)
    //! Controls outcoming bandwidth used by tablet preload.
    (TabletPreloadOut)
    //! Controls outcoming bandwidth used by tablet recovery.
    (TabletRecoveryOut)
    //! Controls incoming bandwidth used by tablet snapshots.
    (TabletSnapshotIn)
    //! Controls incoming bandwidth used by tablet store flush.
    (TabletStoreFlushIn)
    //! Controls outcoming bandwidth used by tablet store flush.
    (TabletStoreFlushOut)
    //! Controls outcoming bandwidth used by tablet replication.
    (TabletReplicationOut)
    //! Controls incoming bandwidth consumed by local jobs.
    (JobIn)
    //! Controls outcoming bandwidth consumed by local jobs.
    (JobOut)
    //! Controls outcoming bandwidth consumed by P2P block distribution.
    (P2POut)
    //! Controls incoming bandwidth consumed by reincarnation jobs.
    (ReincarnationIn)
    //! Controls outcoming bandwidth consumed by reincarnation jobs.
    (ReincarnationOut)
);

DEFINE_ENUM(EChunkLocationThrottlerKind,
    //! Controls incoming location bandwidth used by replication jobs.
    (ReplicationIn)
    //! Controls outcoming location bandwidth used by replication jobs.
    (ReplicationOut)
    //! Controls incoming location bandwidth used by repair jobs.
    (RepairIn)
    //! Controls outcoming location bandwidth used by repair jobs.
    (RepairOut)
    //! Controls incoming location bandwidth used by tablet compaction and partitioning.
    (TabletCompactionAndPartitioningIn)
    //! Controls outcoming location bandwidth used by tablet compaction and partitioning.
    (TabletCompactionAndPartitioningOut)
    //! Controls incoming location bandwidth used by tablet journals.
    (TabletLoggingIn)
    //! Controls outcoming location bandwidth used by tablet journals.
    (TabletLoggingOut)
    //! Controls incoming location bandwidth used by tablet snapshots.
    (TabletSnapshotIn)
    //! Controls incoming location bandwidth used by tablet store flush.
    (TabletStoreFlushIn)
    //! Controls outcoming location bandwidth used by tablet store preload.
    (TabletPreloadOut)
    //! Controls outcoming location bandwidth used by tablet ecovery.
    (TabletRecoveryOut)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
