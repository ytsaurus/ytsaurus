#include "file_node.h"
#include "private.h"
#include "file_node_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
{ }

TFileNode* TFileNode::GetTrunkNode()
{
    return TrunkNode_->As<TFileNode>();
}

const TFileNode* TFileNode::GetTrunkNode() const
{
    return TrunkNode_->As<TFileNode>();
}

void TFileNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, MD5Hasher_);
}

void TFileNode::Load(NCellMaster::TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;

    // COMPAT(ostyakov)
    if (context.GetVersion() >= 627) {
        Load(context, MD5Hasher_);
    }
}

void TFileNode::EndUpload(
    const NChunkClient::NProto::TDataStatistics* statistics,
    const NTableClient::TTableSchema& schema,
    NTableClient::ETableSchemaMode schemaMode,
    TNullable<NTableClient::EOptimizeFor> optimizeFor,
    const TNullable<TMD5Hasher>& md5Hasher)
{
    SetMD5Hasher(md5Hasher);
    TChunkOwnerBase::EndUpload(statistics, schema, schemaMode, optimizeFor, md5Hasher);
}

void TFileNode::GetUploadParams(TNullable<TMD5Hasher>* md5Hasher)
{
    md5Hasher->Assign(GetMD5Hasher());
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public TChunkOwnerTypeHandler<TFileNode>
{
public:
    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TChunkOwnerTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::File;
    }

    virtual bool IsExternalizable() const override
    {
        return true;
    }

protected:
    using TBase = TChunkOwnerTypeHandler<TFileNode>;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateFileNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TFileNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TTransaction* transaction,
        IAttributeDictionary* attributes,
        TAccount* account,
        bool enableAccounting) override
    {
        const auto& config = this->Bootstrap_->GetConfig()->CypressManager;

        auto replicationFactor = attributes->GetAndRemove("replication_factor", config->DefaultFileReplicationFactor);
        auto compressionCodec = attributes->GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::None);
        auto erasureCodec = attributes->GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

        ValidateReplicationFactor(replicationFactor);

        auto nodeHolder = DoCreateImpl(
            id,
            cellTag,
            transaction,
            attributes,
            account,
            enableAccounting,
            replicationFactor,
            compressionCodec,
            erasureCodec);

        auto* node = nodeHolder.get();
        node->SetMD5Hasher(TMD5Hasher());
        return nodeHolder;
    }

    virtual void DoBranch(
        const TFileNode* originatingNode,
        TFileNode* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetMD5Hasher(originatingNode->GetMD5Hasher());
    }

    virtual void DoMerge(
        TFileNode* originatingNode,
        TFileNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->SetMD5Hasher(branchedNode->GetMD5Hasher());
    }

    virtual void DoClone(
        TFileNode* sourceNode,
        TFileNode* clonedNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

        clonedNode->SetMD5Hasher(sourceNode->GetMD5Hasher());
    }
};

INodeTypeHandlerPtr CreateFileTypeHandler(TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

