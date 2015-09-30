#pragma once

#include "public.h"
#include "chunk_tree.h"
#include "chunk_replica.h"

#include <core/misc/property.h>
#include <core/misc/small_vector.h>
#include <core/misc/ref_tracked.h>
#include <core/misc/nullable.h>

#include <core/erasure/public.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_info.pb.h>

#include <server/cell_master/public.h>

#include <server/security_server/cluster_resources.h>

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
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkExportData
{
    ui32 RefCounter : 24;
    // XXX(babenko): to be used later
    ui32 Unused : 8;
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

    typedef SmallVector<TChunkList*, TypicalChunkParentCount> TParents;
    DEFINE_BYREF_RW_PROPERTY(TParents, Parents);

    // This is usually small, e.g. has the length of 3.
    typedef TNodePtrWithIndexList TStoredReplicas;
    DEFINE_BYREF_RO_PROPERTY(TStoredReplicas, StoredReplicas);

    // This list is usually empty.
    // Keeping a holder is very space efficient (takes just 8 bytes).
    typedef std::unique_ptr<yhash_set<TNodePtrWithIndex>> TCachedReplicas;
    DEFINE_BYREF_RO_PROPERTY(TCachedReplicas, CachedReplicas);

public:
    explicit TChunk(const TChunkId& id);

    TChunkDynamicData* GetDynamicData() const;

    TChunkTreeStatistics GetStatistics() const;
    NSecurityServer::TClusterResources GetResourceUsage() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddReplica(TNodePtrWithIndex replica, bool cached);
    void RemoveReplica(TNodePtrWithIndex replica, bool cached);
    TNodePtrWithIndexList GetReplicas() const;

    void ApproveReplica(TNodePtrWithIndex replica);

    void Confirm(
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta);

    bool IsConfirmed() const;
    void ValidateConfirmed();

    bool GetMovable() const;
    void SetMovable(bool value);

    bool GetVital() const;
    void SetVital(bool value);

    bool GetRefreshScheduled() const;
    void SetRefreshScheduled(bool value);

    bool GetPropertiesUpdateScheduled() const;
    void SetPropertiesUpdateScheduled(bool value);

    bool GetSealScheduled() const;
    void SetSealScheduled(bool value);

    const TNullable<TChunkRepairQueueIterator>& GetRepairQueueIterator() const;
    void SetRepairQueueIterator(const TNullable<TChunkRepairQueueIterator>& value);

    void Reset();

    int GetReplicationFactor() const;
    void SetReplicationFactor(int value);

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

    TChunkProperties GetChunkProperties() const;

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
    } Flags_;

    i8 ReplicationFactor_;
    i8 ReadQuorum_;
    i8 WriteQuorum_;
    NErasure::ECodec ErasureCodec_;
    TChunkExportDataList ExportDataList_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

DECLARE_PODTYPE(NYT::NChunkServer::TChunkExportDataList)

#define CHUNK_INL_H_
#include "chunk-inl.h"
#undef CHUNK_INL_H_
