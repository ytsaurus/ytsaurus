#pragma once

#include "public.h"
#include "chunk_tree.h"
#include "chunk_replica.h"

#include <core/misc/property.h>
#include <core/misc/small_vector.h>
#include <core/misc/ref_tracked.h>

#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <core/erasure/public.h>

#include <server/cell_master/public.h>

#include <server/object_server/object_detail.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkProperties
{
    TChunkProperties();

    int ReplicationFactor;
    bool Vital;
};

bool operator== (const TChunkProperties& lhs, const TChunkProperties& rhs);
bool operator!= (const TChunkProperties& lhs, const TChunkProperties& rhs);

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public TChunkTree
    , public NObjectServer::TStagedObject
    , public TRefTracked<TChunk>
{
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkInfo, ChunkInfo);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TMiscExt, MiscExt);

    typedef TSmallVector<TChunkList*, TypicalChunkParentCount> TParents;
    DEFINE_BYREF_RW_PROPERTY(TParents, Parents);

    // This is usually small, e.g. has the length of 3.
    typedef TNodePtrWithIndexList TStoredReplicas;
    DEFINE_BYREF_RO_PROPERTY(TStoredReplicas, StoredReplicas);

    // This list is usually empty.
    // Keeping a holder is very space efficient (takes just 8 bytes).
    typedef std::unique_ptr< yhash_set<TNodePtrWithIndex> > TCachedReplicas;
    DEFINE_BYREF_RO_PROPERTY(TCachedReplicas, CachedReplicas);

    //! Contains a valid iterator for those chunks belonging to the repair queue
    //! and a default-constructed instance for others.
    DEFINE_BYVAL_RW_PROPERTY(TChunkRepairQueueIterator, RepairQueueIterator);

public:
    static const i64 UnknownDiskSpace;

    explicit TChunk(const TChunkId& id);
    ~TChunk();

    TChunkTreeStatistics GetStatistics() const;
    NSecurityServer::TClusterResources GetResourceUsage() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void AddReplica(TNodePtrWithIndex replica, bool cached);
    void RemoveReplica(TNodePtrWithIndex replica, bool cached);
    TSmallVector<TNodePtrWithIndex, TypicalReplicaCount> GetReplicas() const;

    bool ValidateChunkInfo(const NChunkClient::NProto::TChunkInfo& chunkInfo) const;
    
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

    int GetReplicationFactor() const;
    void SetReplicationFactor(int value);

    NErasure::ECodec GetErasureCodec() const;
    void SetErasureCodec(NErasure::ECodec value);

    //! Returns |true| iff erasure codec is not #EErasureCodec::Null.
    bool IsErasure() const;

    //! Returns |true| iff the chunk can be read immediately, i.e. without repair.
    /*!
     *  For regular (non-erasure) chunk this is equivalent to the existence of any replica.
     *  For erasure chunks this is equivalent to the existence of replicas for all data parts.
     */
    bool IsAvailable() const;

    TChunkProperties GetChunkProperties() const;

private:
    struct {
        bool Movable : 1;
        bool Vital : 1;
        bool RefreshScheduled : 1;
        bool PropertiesUpdateScheduled : 1;
    } Flags;

    i16 ReplicationFactor;
    i16 ErasureCodec;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
