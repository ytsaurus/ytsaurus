#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "chunk_tree.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkProperties
{
    int ReplicationFactor = 0;
    bool Vital = false;
};

bool operator== (const TChunkProperties& lhs, const TChunkProperties& rhs);
bool operator!= (const TChunkProperties& lhs, const TChunkProperties& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TChunkDynamicData
    : public NHydra::TEntityDynamicDataBase
{
    struct
    {
        bool RefreshScheduled : 1;
        bool PropertiesUpdateScheduled : 1;
        bool SealScheduled : 1;
    } Flags = {};

    //! Contains a valid iterator for those chunks belonging to the repair queue
    //! and |Null| for others.
    TNullable<TChunkRepairQueueIterator> RepairQueueIterator;

    //! The job that is currently scheduled for this chunk (at most one).
    TJobPtr Job;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkExportData
{
    ui32 RefCounter : 24;
    bool Vital : 1;
    ui8 ReplicationFactor : 7;
};

static_assert(sizeof(TChunkExportData) == 4, "sizeof(TChunkExportData) != 4");
using TChunkExportDataList = TChunkExportData[NObjectClient::MaxSecondaryMasterCells];

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

public:
    explicit TChunk(const TChunkId& id);

    TChunkDynamicData* GetDynamicData() const;

    TChunkTreeStatistics GetStatistics() const;
    NSecurityServer::TClusterResources GetResourceUsage() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddParent(TChunkList* parent);
    void RemoveParent(TChunkList* parent);

    using TCachedReplicas = yhash_set<TNodePtrWithIndex>;
    const TCachedReplicas& CachedReplicas() const;

    using TStoredReplicas = TNodePtrWithIndexList;
    const TStoredReplicas& StoredReplicas() const;

    void AddReplica(TNodePtrWithIndex replica, bool cached);
    void RemoveReplica(TNodePtrWithIndex replica, bool cached);
    TNodePtrWithIndexList GetReplicas() const;

    void ApproveReplica(TNodePtrWithIndex replica);

    void Confirm(
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta);

    bool IsConfirmed() const;

    bool GetMovable() const;
    void SetMovable(bool value);

    bool GetRefreshScheduled() const;
    void SetRefreshScheduled(bool value);

    bool GetPropertiesUpdateScheduled() const;
    void SetPropertiesUpdateScheduled(bool value);

    bool GetSealScheduled() const;
    void SetSealScheduled(bool value);

    const TNullable<TChunkRepairQueueIterator>& GetRepairQueueIterator() const;
    void SetRepairQueueIterator(const TNullable<TChunkRepairQueueIterator>& value);

    bool IsJobScheduled() const;
    TJobPtr GetJob() const;
    void SetJob(TJobPtr job);

    void Reset();

    //! Computes the vitality flag by ORing the local and the external values.
    bool ComputeVital() const;

    //! Computes the replication factor by MAXing the local and the external values.
    int ComputeReplicationFactor() const;

    bool GetLocalVital() const;
    void SetLocalVital(bool value);

    int GetLocalReplicationFactor() const;
    void SetLocalReplicationFactor(int value);

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

    //! Obtains the local properties of the chunk.
    TChunkProperties GetLocalProperties() const;

    //! Updates the local properties of the chunk.
    //! Returns |true| if anything changes.
    bool UpdateLocalProperties(const TChunkProperties& properties);

    //! Updates the properties of the chunk, as seen by the external cell.
    //! Returns |true| if anything changes.
    bool UpdateExternalProprties(int cellIndex, const TChunkProperties& properties);

    //! Returns the maximum number of replicas that can be stored in the same
    //! rack without violating the availability guarantees.
    /*!
     *  \param replicationFactorOverride An override for replication factor;
     *  used when one wants to upload fewer replicas but still guarantee placement safety.
     */
    int GetMaxReplicasPerRack(TNullable<int> replicationFactorOverride) const;

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
    struct {
        bool Movable : 1;
        bool Vital : 1;
    } Flags_ = {};

    ui8 ReplicationFactor_ = 0;
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
