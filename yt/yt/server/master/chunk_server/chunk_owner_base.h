#pragma once

#include "chunk.h"
#include "chunk_merger_traversal_info.h"
#include "chunk_owner_data_statistics.h"

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/server/lib/misc/assert_sizeof.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/table_client/public.h>

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
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode, NChunkClient::EUpdateMode::None);
    DEFINE_BYREF_RW_PROPERTY(TChunkReplication, Replication);
    DEFINE_BYREF_RW_PROPERTY(TChunkReplication, HunkReplication);
    DEFINE_BYREF_RW_PROPERTY(TChunkOwnerDataStatistics, SnapshotStatistics);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TInternedSecurityTags, SnapshotSecurityTags);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TInternedSecurityTags, DeltaSecurityTags);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NCompression::ECodec, CompressionCodec);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NErasure::ECodec, ErasureCodec);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, bool, EnableStripedErasure);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, bool, EnableSkynetSharing);
    DEFINE_CYPRESS_BUILTIN_VERSIONED_ATTRIBUTE(TChunkOwnerBase, NChunkClient::EChunkMergerMode, ChunkMergerMode);

    // TODO(aleksandra-zh): Merger stuff, wrap that.
    // If chunk owner is changed, while it is being merged, it should be marked updated
    // to initiate another merge after the current one is finished.
    DEFINE_BYVAL_RW_PROPERTY(bool, UpdatedSinceLastMerge, false);
    DEFINE_BYREF_RW_PROPERTY(TChunkMergerTraversalInfo, ChunkMergerTraversalInfo);

public:
    using TCypressNode::TCypressNode;
    explicit TChunkOwnerBase(NCypressServer::TVersionedNodeId id);

    TChunkList* GetChunkList() const;
    void SetChunkList(TChunkList* chunkList);

    TChunkList* GetHunkChunkList() const;
    void SetHunkChunkList(TChunkList* chunkList);

    TChunkList* GetChunkList(EChunkListContentType type) const;
    void SetChunkList(EChunkListContentType type, TChunkList* chunkList);

    TChunkLists GetChunkLists() const;

    const TChunkList* GetSnapshotChunkList() const;
    const TChunkList* GetSnapshotHunkChunkList() const;
    const TChunkList* GetSnapshotChunkList(EChunkListContentType type) const;

    const TChunkList* GetDeltaChunkList() const;

    const TChunkOwnerDataStatistics& DeltaStatistics() const;
    TChunkOwnerDataStatistics* MutableDeltaStatistics();

    NSecurityServer::TSecurityTags ComputeSecurityTags() const;

    int GetPrimaryMediumIndex() const;
    void SetPrimaryMediumIndex(int primaryMediumIndex);

    // Hunk-specific primary medium being null (signified by GenericMediumIndex
    // for compactness) means "respect the main (non-hunk-specific) primary
    // medium and replication".
    //
    // Hunk-specific primary medium should be null when and only when
    // hunk-specific replication is empty. Resetting hunk primary medium to null
    // should empty hunk replication, and it should otherwise be impossible to
    // manually make hunk replication empty.
    //
    // Similarly to (main) primary medium, setting a (non-null) hunk primary
    // medium while hunk replication is empty is interpreted as a request to
    // move hunk chunks to that medium. The specified medium is thus
    // automatically added to hunk replication in such case.

    std::optional<int> GetHunkPrimaryMediumIndex() const;
    void SetHunkPrimaryMediumIndex(std::optional<int> hunkPrimaryMediumIndex);
    void ResetHunkPrimaryMediumIndex();

    //! Returns hunk primary medium index unless it's null, in which case
    //! returns (main) primary medium index.
    int GetEffectiveHunkPrimaryMediumIndex() const;

    //! Returns hunk replication unless hunk primary medium index is null, in
    //! which case returns (main) replication.
    const TChunkReplication& EffectiveHunkReplication() const;

    // COMPAT(h0pless): Once clients are sending table schema options during begin upload:
    //   - move fields from TCommonUploadContext to TBeginUploadContext;
    //   - remove inheritance by TEndUploadContext;
    //   - delete ParseCommonUploadContext function.
    struct TCommonUploadContext
    {
        explicit TCommonUploadContext(NCellMaster::TBootstrap* bootstrap);

        NChunkClient::EUpdateMode Mode = {};
        NTableServer::TMasterTableSchema* TableSchema = nullptr;
        std::optional<NTableClient::ETableSchemaMode> SchemaMode = NTableClient::ETableSchemaMode::Weak;

        NCellMaster::TBootstrap* const Bootstrap = nullptr;
    };

    virtual void ParseCommonUploadContext(const TCommonUploadContext& context);

    struct TBeginUploadContext
        : public TCommonUploadContext
    {
        NTableServer::TMasterTableSchema* ChunkSchema = nullptr;
        explicit TBeginUploadContext(NCellMaster::TBootstrap* bootstrap);
    };

    virtual void BeginUpload(const TBeginUploadContext& context);

    struct TEndUploadContext
        : public TCommonUploadContext
    {
        // COMPAT (h0pless): remove this when clients will send table schema options during begin upload
        explicit TEndUploadContext(NCellMaster::TBootstrap* bootstrap);

        std::optional<NTableClient::EOptimizeFor> OptimizeFor;
        std::optional<NCompression::ECodec> CompressionCodec;
        std::optional<NErasure::ECodec> ErasureCodec;
        std::optional<TChunkOwnerDataStatistics> Statistics;
        std::optional<NChunkClient::EChunkFormat> ChunkFormat;
        std::optional<NCrypto::TMD5Hasher> MD5Hasher;
        NSecurityServer::TInternedSecurityTags SecurityTags;
    };

    virtual void EndUpload(const TEndUploadContext& context);
    virtual void GetUploadParams(std::optional<NCrypto::TMD5Hasher>* md5Hasher);
    virtual bool IsSorted() const;

    NYTree::ENodeType GetNodeType() const override;

    NSecurityServer::TClusterResources GetDeltaResourceUsage() const override;
    NSecurityServer::TClusterResources GetTotalResourceUsage() const override;

    TChunkOwnerDataStatistics ComputeTotalStatistics() const;

    bool HasDataWeight() const;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

private:
    int PrimaryMediumIndex_ = NChunkClient::DefaultStoreMediumIndex;
    int HunkPrimaryMediumIndex_ = NChunkClient::GenericMediumIndex;
    std::unique_ptr<TChunkOwnerDataStatistics> DeltaStatistics_;
    TEnumIndexedArray<EChunkListContentType, TChunkListPtr> ChunkLists_;

    TChunkOwnerDataStatistics ComputeUpdateStatistics() const;

    NSecurityServer::TClusterResources GetDiskUsage(const TChunkOwnerDataStatistics& statistics) const;
};

DEFINE_MASTER_OBJECT_TYPE(TChunkOwnerBase)

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TChunkOwnerBase, 608);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
