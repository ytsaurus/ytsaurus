#include "link_node_type_handler.h"
#include "link_node.h"
#include "link_node_proxy.h"
#include "shard.h"
#include "portal_exit_node.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/master/object_server/path_resolver.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TLinkNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TLinkNode>;

public:
    using TBase::TBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Link;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TLinkNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateLinkNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TLinkNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto originalTargetPath = context.ExplicitAttributes->GetAndRemove<TString>("target_path");

        auto enableSymlinkCyclicityCheck = GetDynamicCypressManagerConfig()->EnableSymlinkCyclicityCheck;
        if (enableSymlinkCyclicityCheck) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            auto originalLinkPath = cypressManager->GetNodePath(context.ServiceTrunkNode, context.Transaction) + context.UnresolvedPathSuffix;

            auto* shard = context.Shard;
            //  Make sure originalLinkPath and originalTargetPath get resolved properly.
            auto linkPath = shard->MaybeRewritePath(originalLinkPath);
            auto targetPath = shard->MaybeRewritePath(originalTargetPath);

            static const TString nullService;
            static const TString nullMethod;

            TPathResolver linkPathResolver(Bootstrap_, nullService, nullMethod, linkPath, context.Transaction);
            TPathResolver targetPathResolver(Bootstrap_, nullService, nullMethod, targetPath, context.Transaction);

            auto linkPathResolveResult = linkPathResolver.Resolve(TPathResolverOptions());
            auto targetPathResolveResult = targetPathResolver.Resolve(TPathResolverOptions());

            auto getPayloadObject = [&] (TPathResolver::TResolveResult result) -> TCypressNode* {
                auto payload = std::get_if<TPathResolver::TLocalObjectPayload>(&result.Payload);
                if (payload) {
                    return dynamic_cast<TCypressNode*>(payload->Object);
                }
                return nullptr;
            };

            TCypressNode* linkPathObject = getPayloadObject(linkPathResolveResult);
            TCypressNode* targetPathObject =  getPayloadObject(targetPathResolveResult);

            if (linkPathObject && targetPathObject) {
                auto canonicalLinkPath = cypressManager->GetNodePath(linkPathObject->GetTrunkNode(), linkPathObject->GetTransaction());
                auto canonicalTargetPath = cypressManager->GetNodePath(targetPathObject->GetTrunkNode(), targetPathObject->GetTransaction());

                canonicalLinkPath += linkPathResolveResult.UnresolvedPathSuffix;
                canonicalTargetPath += targetPathResolveResult.UnresolvedPathSuffix;

                if (canonicalLinkPath == canonicalTargetPath) {
                    THROW_ERROR_EXCEPTION("Failed to create link: link is cyclic")
                        << TErrorAttribute("target_path", targetPath)
                        << TErrorAttribute("path", originalLinkPath);
                }
            }
        }

        auto implHolder = TBase::DoCreate(id, context);
        implHolder->SetTargetPath(originalTargetPath);

        YT_LOG_DEBUG("Link created (LinkId: %v, TargetPath: %v)",
            id,
            originalTargetPath);

        return implHolder;
    }

    void DoBranch(
        const TLinkNode* originatingNode,
        TLinkNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetTargetPath(originatingNode->GetTargetPath());
    }

    void DoMerge(
        TLinkNode* originatingNode,
        TLinkNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->SetTargetPath(branchedNode->GetTargetPath());
    }

    void DoClone(
        TLinkNode* sourceNode,
        TLinkNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->SetTargetPath(sourceNode->GetTargetPath());
    }

    bool HasBranchedChangesImpl(
        TLinkNode* originatingNode,
        TLinkNode* branchedNode) override
    {
        if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
            return true;
        }

        return branchedNode->GetTargetPath() != originatingNode->GetTargetPath();
    }

    void DoBeginCopy(
        TLinkNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->GetTargetPath());
    }

    void DoEndCopy(
        TLinkNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);

        using NYT::Load;
        trunkNode->SetTargetPath(Load<TYPath>(*context));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLinkNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TLinkNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
