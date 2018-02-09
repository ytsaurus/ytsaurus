#pragma once

#include "public.h"
#include "private.h"

#include "chunk.h"

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/crypto/crypto.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base classes for Cypress nodes that own chunks.
class TChunkOwnerBase
    : public NCypressServer::TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode, NChunkClient::EUpdateMode::None);
    DEFINE_BYREF_RW_PROPERTY(TChunkProperties, Properties);
    DEFINE_BYVAL_RW_PROPERTY(int, PrimaryMediumIndex, NChunkClient::DefaultStoreMediumIndex);
    //! Only makes sense for branched nodes.
    //! If |true| then properties update will be performed for the newly added chunks upon top-level commit.
    DEFINE_BYVAL_RW_PROPERTY(bool, ChunkPropertiesUpdateNeeded, false);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, SnapshotStatistics);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, DeltaStatistics);

    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NCompression::ECodec, CompressionCodec);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NErasure::ECodec, ErasureCodec);

public:
    explicit TChunkOwnerBase(const NCypressServer::TVersionedNodeId& id);

    const TChunkList* GetSnapshotChunkList() const;
    const TChunkList* GetDeltaChunkList() const;

    virtual void BeginUpload(NChunkClient::EUpdateMode mode);
    virtual void EndUpload(
        const NChunkClient::NProto::TDataStatistics* statistics,
        const NTableClient::TTableSchema& schema,
        NTableClient::ETableSchemaMode schemaMode,
        TNullable<NTableClient::EOptimizeFor> optimizeFor,
        const TNullable<TMD5Hasher>& md5Hasher);
    virtual void GetUploadParams(TNullable<TMD5Hasher>* md5Hasher);
    virtual bool IsSorted() const;

    virtual NYTree::ENodeType GetNodeType() const override;

    NChunkClient::NProto::TDataStatistics ComputeTotalStatistics() const;
    NChunkClient::NProto::TDataStatistics ComputeUpdateStatistics() const;

    bool HasDataWeight() const;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
