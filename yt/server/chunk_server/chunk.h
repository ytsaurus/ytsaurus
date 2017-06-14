#pragma once

#include "public.h"
#include "chunk_properties.h"
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
    TChunkProperties Properties;
};

static_assert(sizeof(TChunkExportData) == sizeof(TChunkProperties) + 4, "sizeof(TChunkExportData) != sizeof(TChunkProperties) + 4");
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
    //! and |Null| for others.
    std::array<TNullable<TChunkRepairQueueIterator>, MaxMediumCount> RepairQueueIterators;

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
    DEFINE_BYREF_RW_PROPERTY(TChunkProperties, LocalProperties);

public:
    explicit TChunk(const TChunkId& id);

    TChunkDynamicData* GetDynamicData() const;

    TChunkTreeStatistics GetStatistics() const;
    NSecurityServer::TClusterResources GetResourceUsage() const;

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

    using TCachedReplicas = yhash_set<TNodePtrWithIndexes>;
    const TCachedReplicas& CachedReplicas() const;

    using TStoredReplicas = TNodePtrWithIndexesList;
    const TStoredReplicas& StoredReplicas() const;

    void AddReplica(TNodePtrWithIndexes replica, bool cached);
    void RemoveReplica(TNodePtrWithIndexes replica, bool cached);
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

    const TNullable<TChunkRepairQueueIterator>& GetRepairQueueIterator(int mediumIndex) const;
    void SetRepairQueueIterator(int mediumIndex, const TNullable<TChunkRepairQueueIterator>& value);

    bool IsJobScheduled() const;
    TJobPtr GetJob() const;
    void SetJob(TJobPtr job);

    //! Computes the vitality flag by ORing the local and the external values.
    bool ComputeVital() const;

    bool GetLocalVital() const;
    void SetLocalVital(bool value);

    //! Computes properties of the chunk by combining local and external values.
    //! For semantics of combining, see #TChunkProperties::operator|=().
    TChunkProperties ComputeProperties() const;

    //! Computes the replication factor for the specified medium by combining the
    //! local and the external values. See #ComputeProperties(int).
    int ComputeReplicationFactor(int mediumIndex) const;

    //! Computes the replication factors for all media by combining the local and
    //! the external values. See #ComputeProperties(int).
    //! NB: most of the time, most of the elements of the returned array will be zero.
    TPerMediumIntArray ComputeReplicationFactors() const;

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

    //! Provides read-only access to external properties.
    const TChunkProperties& ExternalProperties(int cellIndex) const;

    //! Provides write access to external properties.
    TChunkProperties& ExternalProperties(int cellIndex);

    //! Returns the maximum number of replicas that can be stored in the same
    //! rack without violating the availability guarantees.
    /*!
     *  \param replicationFactorOverride An override for replication factor;
     *  used when one wants to upload fewer replicas but still guarantee placement safety.
     */
    int GetMaxReplicasPerRack(int mediumIndex, TNullable<int> replicationFactorOverride) const;

    //! Returns the export data w.r.t. to a cell with a given #index.
    /*!
     *  \see #TMultiCellManager::GetRegisteredMasterCellIndex
     */
    const TChunkExportData& GetExportData(int cellIndex) const;

    //! Increments export ref counter.
    void Export(int cellIndex);

    //! Decrements export ref counter.
    void Unexport(int cellIndex, int importRefCounter);

private:
    ui8 ReadQuorum_ = 0;
    ui8 WriteQuorum_ = 0;
    NErasure::ECodec ErasureCodec_ = NErasure::ECodec::None;

    //! The number of non-empty entries in #ExportDataList_.
    ui8 ExportCounter_ = 0;
    //! Per-cell data, indexed by cell index; cf. TMulticellManager::GetRegisteredMasterCellIndex.
    TChunkExportDataList ExportDataList_ = {};

    //! This list is usually empty. Keeping a holder is very space efficient.
    std::unique_ptr<TCachedReplicas> CachedReplicas_;
    static const TCachedReplicas EmptyCachedReplicas;

    //! This additional indirection helps to save up some space since
    //! no replicas are being maintained for foreign chunks.
    std::unique_ptr<TStoredReplicas> StoredReplicas_;
    static const TStoredReplicas EmptyStoredReplicas;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

Y_DECLARE_PODTYPE(NYT::NChunkServer::TChunkExportDataList);

#define CHUNK_INL_H_
#include "chunk-inl.h"
#undef CHUNK_INL_H_
