#include "file_node_type_handler.h"
#include "file_node.h"
#include "file_node_proxy.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/chunk_server/chunk_owner_type_handler.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/crypto/crypto.h>

namespace NYT::NFileServer {

using namespace NCrypto;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public TChunkOwnerTypeHandler<TFileNode>
{
private:
    using TBase = TChunkOwnerTypeHandler<TFileNode>;

public:
    using TBase::TBase;

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::File;
    }

protected:
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
        const TCreateNodeContext& context) override
    {
        const auto& config = Bootstrap_->GetConfig()->CypressManager;
        auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);
        auto replicationFactor = combinedAttributes.GetAndRemove("replication_factor", config->DefaultFileReplicationFactor);
        auto compressionCodec = combinedAttributes.GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::None);
        auto erasureCodec = combinedAttributes.GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

        ValidateReplicationFactor(replicationFactor);

        auto nodeHolder = DoCreateImpl(
            id,
            context,
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
        const TLockRequest& lockRequest) override
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
        TFileNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->SetMD5Hasher(sourceNode->GetMD5Hasher());
    }

    virtual void DoBeginCopy(
        TFileNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->GetMD5Hasher());
    }

    virtual void DoEndCopy(
        TFileNode* node,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(node, context, factory);

        using NYT::Load;
        node->SetMD5Hasher(Load<std::optional<TMD5Hasher>>(*context));
    }
};

INodeTypeHandlerPtr CreateFileTypeHandler(TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileServer

