#pragma once

#include "public.h"
#include "private.h"

#include "chunk.h"

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Base classes for cypress nodes that own chunks.
class TChunkOwnerBase
    : public NCypressServer::TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode);
    DEFINE_BYREF_RW_PROPERTY(TChunkProperties, Properties);
    DEFINE_BYVAL_RO_PROPERTY(int, PrimaryMediumIndex);
    //! Only makes sense for branched nodes.
    //! If |true| then properties update will be performed for the newly added chunks upon top-level commit.
    DEFINE_BYVAL_RW_PROPERTY(bool, ChunkPropertiesUpdateNeeded);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, SnapshotStatistics);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, DeltaStatistics);

    int GetReplicationFactor(int mediumIndex) const;
    void SetReplicationFactorOrThrow(int mediumIndex, int replicationFactor);

    bool GetDataPartsOnly(int mediumIndex) const;
    void SetDataPartsOnlyOrThrow(int mediumIndex, bool dataPartsOnly);

    bool GetVital() const;
    void SetVital(bool vital);

    int GetPrimaryMediumReplicationFactor() const;
    void SetPrimaryMediumReplicationFactorOrThrow(int replicationFactor);

    int GetPrimaryMediumDataPartsOnly() const;

    void SetPrimaryMediumIndexOrThrow(int mediumIndex);
    void SetPropertiesOrThrow(const TChunkProperties& props);
    // Since every Set*OrThrow method validates current state before applying
    // necessary changes, we need to be able to modify both properties and
    // primary medium index at once.
    void SetPrimaryMediumIndexAndPropertiesOrThrow(
        int primaryMediumIndex,
        const TChunkProperties& props);

public:
    explicit TChunkOwnerBase(const NCypressServer::TVersionedNodeId& id);

    const TChunkList* GetSnapshotChunkList() const;
    const TChunkList* GetDeltaChunkList() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode);
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const NTableClient::TTableSchema& schema,
        NTableClient::ETableSchemaMode schemaMode);
    virtual bool IsSorted() const;

    virtual NYTree::ENodeType GetNodeType() const override;

    NChunkClient::NProto::TDataStatistics ComputeTotalStatistics() const;
    NChunkClient::NProto::TDataStatistics ComputeUpdateStatistics() const;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

private:
    static void ValidateMedia(const TChunkProperties& props, int primaryMediumIndex);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
