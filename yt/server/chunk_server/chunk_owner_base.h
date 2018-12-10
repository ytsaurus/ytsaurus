#pragma once

#include "public.h"
#include "private.h"

#include "chunk.h"

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/public.h>

#include <yt/server/table_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/client/chunk_client/proto/data_statistics.pb.h>

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
    using TBase = NCypressServer::TCypressNodeBase;

    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode, NChunkClient::EUpdateMode::None);
    DEFINE_BYREF_RW_PROPERTY(TChunkReplication, Replication);
    DEFINE_BYVAL_RW_PROPERTY(int, PrimaryMediumIndex, NChunkClient::DefaultStoreMediumIndex);
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
        const NTableServer::TSharedTableSchemaPtr& schema,
        NTableClient::ETableSchemaMode schemaMode,
        std::optional<NTableClient::EOptimizeFor> optimizeFor,
        const std::optional<NCrypto::TMD5Hasher>& md5Hasher);
    virtual void GetUploadParams(std::optional<NCrypto::TMD5Hasher>* md5Hasher);
    virtual bool IsSorted() const;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    virtual NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

    NChunkClient::NProto::TDataStatistics ComputeTotalStatistics() const;

    bool HasDataWeight() const;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

private:
    NChunkClient::NProto::TDataStatistics ComputeUpdateStatistics() const;

    NSecurityServer::TClusterResources GetDiskUsage(const NChunkClient::NProto::TDataStatistics& statistics) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
