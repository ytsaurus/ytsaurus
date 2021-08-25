#pragma once

#include "chunk.h"

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/intern_registry.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base classes for Cypress nodes that own chunks.
class TChunkOwnerBase
    : public NCypressServer::TCypressNode
{
public:
    using TBase = NCypressServer::TCypressNode;

    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode, NChunkClient::EUpdateMode::None);
    DEFINE_BYREF_RW_PROPERTY(TChunkReplication, Replication);
    DEFINE_BYVAL_RW_PROPERTY(int, PrimaryMediumIndex, NChunkClient::DefaultStoreMediumIndex);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, SnapshotStatistics);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TInternedSecurityTags, SnapshotSecurityTags);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TDataStatistics, DeltaStatistics);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TInternedSecurityTags, DeltaSecurityTags);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NCompression::ECodec, CompressionCodec);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NErasure::ECodec, ErasureCodec);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, bool, EnableSkynetSharing);
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableChunkMerger, false);
    // If chunk owner is changed, while it is being merged, it should be marked updated
    // to initiate another merge after the current one is finished.
    DEFINE_BYVAL_RW_PROPERTY(bool, UpdatedSinceLastMerge, false);

public:
    explicit TChunkOwnerBase(NCypressServer::TVersionedNodeId id);

    const TChunkList* GetSnapshotChunkList() const;
    const TChunkList* GetDeltaChunkList() const;

    NSecurityServer::TSecurityTags GetSecurityTags() const;

    struct TBeginUploadContext
    {
        NChunkClient::EUpdateMode Mode;
    };

    virtual void BeginUpload(const TBeginUploadContext& context);

    struct TEndUploadContext
    {
        explicit TEndUploadContext(NCellMaster::TBootstrap* bootstrap);

        std::optional<NCompression::ECodec> CompressionCodec;
        std::optional<NErasure::ECodec> ErasureCodec;
        const NChunkClient::NProto::TDataStatistics* Statistics = nullptr;
        NTableServer::TMasterTableSchema* Schema;
        NTableClient::ETableSchemaMode SchemaMode = NTableClient::ETableSchemaMode::Weak;
        std::optional<NTableClient::EOptimizeFor> OptimizeFor;
        std::optional<NCrypto::TMD5Hasher> MD5Hasher;
        NSecurityServer::TInternedSecurityTags SecurityTags;

        NCellMaster::TBootstrap* const Bootstrap;
    };

    virtual void EndUpload(const TEndUploadContext& context);
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

} // namespace NYT::NChunkServer
