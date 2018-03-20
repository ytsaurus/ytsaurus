#pragma once

#include "public.h"
#include "chunk_requisition.h"
#include "chunk_replica.h"
#include "chunk_tree.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/format.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/intrusive_linked_list.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkExportData
{
    ui32 RefCounter;
    TChunkRequisitionIndex ChunkRequisitionIndex;
};

static_assert(sizeof(TChunkExportData) == 8, "sizeof(TChunkExportData) != 8");
using TChunkExportDataList = TChunkExportData[NObjectClient::MaxSecondaryMasterCells];

////////////////////////////////////////////////////////////////////////////////

struct TChunkDynamicData
    : public NObjectServer::TObjectDynamicData
{
    //! Indicates that certain background scans were scheduled for this chunk.
    EChunkScanKind ScanFlags = EChunkScanKind::None;

    //! Indicates for which epoch #ScanFlags are valid.
    NObjectServer::TEpoch ScanEpoch = 0;

    //! For each medium, contains a valid iterator for those chunks belonging to the repair queue
    //! and null (default iterator value) for others.
    std::array<TChunkRepairQueueIterator, MaxMediumCount> RepairQueueIterators;

    //! The job that is currently scheduled for this chunk (at most one).
    TJobPtr Job;

    //! All chunks are linked via this node.
    TIntrusiveLinkedListNode<TChunk> AllLinkedListNode;

    //! All journal chunks are linked via this node.
    TIntrusiveLinkedListNode<TChunk> JournalLinkedListNode;
};

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public TChunkTree
    , public TRefTracked<TChunk>
{
public:
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkInfo, ChunkInfo);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TMiscExt, MiscExt);

    // This list is typically small, e.g. has the length of 1.
    // It may contain duplicates, i.e. when a chunk is added into the same
    // table multiple times during merge.
    using TParents = SmallVector<TChunkList*, TypicalChunkParentCount>;
    DEFINE_BYREF_RO_PROPERTY(TParents, Parents);

    DEFINE_BYVAL_RW_PROPERTY(bool, Movable);

public:
    explicit TChunk(const TChunkId& id);

    TChunkDynamicData* GetDynamicData() const;

    TChunkTreeStatistics GetStatistics() const;

    //! Get disk size of a single part of the chunk.
    /*!
     *  For a non-erasure chunk, simply returns its size
     *  (same as ChunkInfo().disk_space()).
     *  For an erasure chunk, returns that size divided by the number of parts
     *  used by the codec.
     */
    i64 GetPartDiskSpace() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddParent(TChunkList* parent);
    void RemoveParent(TChunkList* parent);

    using TCachedReplicas = THashSet<TNodePtrWithIndexes>;
    const TCachedReplicas& CachedReplicas() const;

    using TStoredReplicas = TNodePtrWithIndexesList;
    const TStoredReplicas& StoredReplicas() const;

    using TLastSeenReplicas = std::array<TNodeId, LastSeenReplicaCount>;
    //! For non-erasure chunks, contains a FIFO queue of seen replicas; its tail position is kept in #CurrentLastSeenReplicaIndex_.
    //! For erasure chunks, this array is directly addressed by replica indexes; at most one replica is kept per part.
    const TLastSeenReplicas& LastSeenReplicas() const;

    void AddReplica(TNodePtrWithIndexes replica, const TMedium* medium);
    void RemoveReplica(TNodePtrWithIndexes replica, const TMedium* medium);
    TNodePtrWithIndexesList GetReplicas() const;

    void ApproveReplica(TNodePtrWithIndexes replica);

    void Confirm(
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta);

    bool IsConfirmed() const;

    bool GetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch) const;
    void SetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch);
    void ClearScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch);
    TChunk* GetNextScannedChunk(EChunkScanKind kind) const;

    TChunkRepairQueueIterator GetRepairQueueIterator(int mediumIndex) const;
    void SetRepairQueueIterator(int mediumIndex, TChunkRepairQueueIterator value);

    bool IsJobScheduled() const;
    TJobPtr GetJob() const;
    void SetJob(TJobPtr job);

    //! Refs all (local and external) requisitions this chunk uses.
    //! Supposed to be called soon after the chunk is constructed or loaded.
    void RefUsedRequisitions(TChunkRequisitionRegistry* registry) const;

    //! A reciprocal to the above. Called at chunk destruction.
    void UnrefUsedRequisitions(
        TChunkRequisitionRegistry* registry,
        const NObjectServer::TObjectManagerPtr& objectManager) const;

    TChunkRequisitionIndex GetLocalRequisitionIndex() const;
    void SetLocalRequisitionIndex(
        TChunkRequisitionIndex requisitionIndex,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::TObjectManagerPtr& objectManager);

    TChunkRequisitionIndex GetExternalRequisitionIndex(int cellIndex) const;
    void SetExternalRequisitionIndex(
        int cellIndex,
        TChunkRequisitionIndex requisitionIndex,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::TObjectManagerPtr& objectManager);

    //! Computes chunk's requisition by combining local and external values.
    //! For semantics of combining, see #TChunkRequisition::operator|=().
    TChunkRequisition ComputeRequisition(const TChunkRequisitionRegistry* registry) const;

    //! Computes chunk's replication by combining local and external values.
    //! For semantics of combining, see #TChunkReplication::operator|=().
    /*!
     *  NB: by default only COMMITTED OWNERS affect this. If the chunk has no
     *  committed owners, then non-committed ones are taken into account.
     *
     *  If there're no owners at all, Null is returned. Usually this means that
     *  this is a newly created staged chunk.
     */
    TNullable<TChunkReplication> ComputeReplication(const TChunkRequisitionRegistry* registry) const;

    //! Computes the replication factor for the specified medium by combining the
    //! local and the external values. See #ComputeReplication().
    TNullable<int> ComputeReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const;

    //! Computes the replication factors for all media by combining the local and
    //! the external values. See #ComputeReplication().
    //! NB: most of the time, most of the elements of the returned array will be zero.
    TNullable<TPerMediumIntArray> ComputeReplicationFactors(const TChunkRequisitionRegistry* registry) const;

    int GetReadQuorum() const;
    void SetReadQuorum(int value);

    int GetWriteQuorum() const;
    void SetWriteQuorum(int value);

    NErasure::ECodec GetErasureCodec() const;
    void SetErasureCodec(NErasure::ECodec value);

    //! Returns |true| iff this is an erasure chunk.
    bool IsErasure() const;

    //! Returns |true| iff this is a journal chunk.
    bool IsJournal() const;

    //! Returns |true| iff this is a regular chunk.
    bool IsRegular() const;

    //! Returns |true| iff the chunk can be read immediately, i.e. without repair.
    /*!
     *  For regular (non-erasure) chunk this is equivalent to the existence of any replica.
     *  For erasure chunks this is equivalent to the existence of replicas for all data parts.
     */
    bool IsAvailable() const;

    //! Returns |true| iff this is a sealed journal chunk.
    //! For blob chunks always returns |true|.
    bool IsSealed() const;

    //! Returns the number of rows in a sealed chunk.
    i64 GetSealedRowCount() const;

    //! Marks the chunk as sealed, i.e. sets its ultimate row count, data size etc.
    void Seal(const NChunkClient::NProto::TMiscExt& info);

    //! For journal chunks, returns true iff the chunk is sealed.
    //! For non-journal chunks, return true iff the chunk is confirmed.
    bool IsDiskSizeFinal() const;

    //! Returns the maximum number of replicas that can be stored in the same
    //! rack without violating the availability guarantees.
    /*!
     *  As #ComputeReplication(), takes into account only committed owners of
     *  this chunk, if there're any. Otherwise falls back to all owners.
     *
     *  \param replicationFactorOverride An override for replication factor;
     *  used when one wants to upload fewer replicas but still guarantee placement safety.
     */
    TNullable<int> GetMaxReplicasPerRack(
        int mediumIndex,
        TNullable<int> replicationFactorOverride,
        const TChunkRequisitionRegistry* registry) const;

    //! Returns the export data w.r.t. to a cell with a given #index.
    /*!
     *  \see #TMulticellManager::GetRegisteredMasterCellIndex
     */
    const TChunkExportData& GetExportData(int cellIndex) const;

    int ExportCounter() const;

    //! Increments export ref counter.
    void Export(int cellIndex, TChunkRequisitionRegistry* registry);

    //! Decrements export ref counter.
    void Unexport(
        int cellIndex,
        int importRefCounter,
        TChunkRequisitionRegistry* registry,
        const NObjectServer::TObjectManagerPtr& objectManager);

private:
    ui8 ReadQuorum_ = 0;
    ui8 WriteQuorum_ = 0;
    NErasure::ECodec ErasureCodec_ = NErasure::ECodec::None;

    TChunkRequisitionIndex LocalRequisitionIndex_;

    //! The number of non-empty entries in #ExportDataList_.
    ui8 ExportCounter_ = 0;

    //! Per-cell data, indexed by cell index; cf. TMulticellManager::GetRegisteredMasterCellIndex.
    TChunkExportDataList ExportDataList_ = {};

    struct TReplicasData
    {
        //! This set is usually empty. Keeping a holder is very space efficient.
        std::unique_ptr<TCachedReplicas> CachedReplicas;

        //! Just all the stored replicas.
        TStoredReplicas StoredReplicas;

        //! Null entries are InvalidNodeId.
        TLastSeenReplicas LastSeenReplicas;
        //! Indicates the position in LastSeenReplicas to be written next.
        int CurrentLastSeenReplicaIndex = 0;
    };

    //! This additional indirection helps to save up some space since
    //! no replicas are being maintained for foreign chunks.
    //! Is also separates relatively mutable data from static one,
    //! which helps to avoid excessive CoW during snapshot construction.
    std::unique_ptr<TReplicasData> ReplicasData_;

    const TReplicasData& ReplicasData() const;
    TReplicasData* MutableReplicasData();

    static const TCachedReplicas EmptyCachedReplicas;
    static const TReplicasData EmptyReplicasData;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

Y_DECLARE_PODTYPE(NYT::NChunkServer::TChunkExportDataList);

#define CHUNK_INL_H_
#include "chunk-inl.h"
#undef CHUNK_INL_H_
