#include "link_node_type_handler.h"
#include "link_node.h"
#include "link_node_proxy.h"
#include "shard.h"
#include "portal_exit_node.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/master/object_server/path_resolver.h>

#include <yt/yt/server/master/sequoia_server/sequoia_queue_manager.h>

#include <yt/yt/ytlib/sequoia_client/helpers.cpp>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NSequoiaClient;
using namespace NTableClient;

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
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TLinkNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        const auto& cypressManager = GetBootstrap()->GetCypressManager();
        auto originalTargetPath = context.ExplicitAttributes->GetAndRemove<TString>("target_path");
        auto originalLinkPath = cypressManager->GetNodePath(context.ServiceTrunkNode, context.Transaction) + context.UnresolvedPathSuffix;

        auto enableSymlinkCyclicityCheck = GetDynamicCypressManagerConfig()->EnableSymlinkCyclicityCheck;
        if (enableSymlinkCyclicityCheck) {
            //  Make sure originalLinkPath and originalTargetPath get resolved properly.
            auto* shard = context.Shard;
            auto linkPath = shard->MaybeRewritePath(originalLinkPath);
            auto targetPath = shard->MaybeRewritePath(originalTargetPath);

            static const TString nullService;
            static const TString nullMethod;

            auto getPayloadObject = [&] (TPathResolver::TResolveResult result) -> TCypressNode* {
                auto payload = std::get_if<TPathResolver::TLocalObjectPayload>(&result.Payload);
                if (payload) {
                    return dynamic_cast<TCypressNode*>(payload->Object);
                }
                return nullptr;
            };

            // If original path is a symlink - a "&" should be added to avoid resolving it further.
            if (!linkPath.EndsWith("&")) {
                linkPath += "&";
            }

            TPathResolver linkPathResolver(GetBootstrap(), nullService, nullMethod, linkPath, context.Transaction);
            auto linkPathResolveResult = linkPathResolver.Resolve(TPathResolverOptions());

            TCypressNode* linkPathObject = getPayloadObject(linkPathResolveResult);

            TString canonicalLinkPath;
            if (linkPathObject) {
                canonicalLinkPath = cypressManager->GetNodePath(linkPathObject->GetTrunkNode(), linkPathObject->GetTransaction());
                canonicalLinkPath += linkPathResolveResult.UnresolvedPathSuffix;

                // Since "&" was added before, we might need to remove it here.
                if (canonicalLinkPath.EndsWith("&")) {
                    canonicalLinkPath.pop_back();
                }
            }

            auto incrementalResolveWithCheck = [&] (const TString& pathToResolve, const TString& forbiddenPrefix) {
                auto currentResolvePath = pathToResolve;
                TPathResolverOptions options;

                // On the first iteration of the loop we need to stop on the first symlink we encounter,
                // on every consecutive pass the first symlink has to be ignored,
                // since it's going to be the symlink that was checked on the previous iteration.
                options.SymlinkEncounterCountLimit = 1;

                // Using this as a bootleg flag to see if we hit the end of the resolve loop.
                TString previousResolvedPath;

                while (true) {
                    // Resolving currentResolvePath before we hit the 1st symlink after it.
                    // Ex.: Let's imagine that the path //tmp/node1/symlink1/node2/symlink2/symlink3/node3 is passed here.
                    // 1st resolve: //tmp/node1/symlink1
                    // 2nd resolve: //tmp/node1/symlink1/node2/symlink2
                    // 3rd resolve: //tmp/node1/symlink1/node2/symlink2/symlink3
                    // 4th resolve: //tmp/node1/symlink1/node2/symlink2/symlink3/node3
                    // 5th resolve: //tmp/node1/symlink1/node2/symlink2/symlink3/node3
                    // Resolve 4 and 5 returned the same object -> stop the resolve loop.
                    TPathResolver pathResolver(GetBootstrap(), nullService, nullMethod, currentResolvePath, context.Transaction);
                    auto pathResolveResult = pathResolver.Resolve(options);
                    auto pathObject = getPayloadObject(pathResolveResult);
                    // Patching resolve depth to make sure we don't go into an infinite loop.
                    options.InitialResolveDepth = pathResolveResult.ResolveDepth;

                    if (!pathObject) {
                        return true;
                    }

                    // Comparing resolved prefix to check if it matches the forbidden one.
                    auto resolvedPrefix = cypressManager->GetNodePath(pathObject->GetTrunkNode(), pathObject->GetTransaction());
                    if (resolvedPrefix == forbiddenPrefix) {
                        return false;
                    }

                    // Comparing previous resolved path and current resolved path.
                    // This is the check that determines if the resolve loop is complete.
                    auto currentResolvedPath = resolvedPrefix + pathResolveResult.UnresolvedPathSuffix;
                    if (previousResolvedPath == currentResolvedPath) {
                        // One final check that yet non-existing path that is being created does not match the target path.
                        return currentResolvedPath != forbiddenPrefix;
                    }

                    previousResolvedPath = currentResolvedPath;

                    // Patching currentResolvePath.
                    // Ex.: After the first resolve the patch would look like this:
                    // tmp/node1/symlink1/node2/symlink2/symlink3/node3 -> #<SYMLINK1_ID>/node2/symlink2/symlink3/node3
                    // This allows resolver to skip verified parts of the path by going directly to the last checked node by id.
                    auto resolvedTargetPathNodeId = pathObject->GetTrunkNode()->GetId();
                    currentResolvePath = FromObjectId(resolvedTargetPathNodeId) + pathResolveResult.UnresolvedPathSuffix;

                    // If pathObject is not a link - everything will work just fine, this option will just not be used by the resolver.
                    // If pathObject is a link, first resolve over it should be ignored for us to go any deeper.
                    options.SymlinkEncounterCountLimit = 2;
                }
            };

            if (!incrementalResolveWithCheck(targetPath, canonicalLinkPath)) {
                THROW_ERROR_EXCEPTION("Failed to create link: link is cyclic")
                    <<TErrorAttribute("target_path", targetPath)
                    << TErrorAttribute("path", originalLinkPath);
            }
        }

        auto implHolder = TBase::DoCreate(id, context);
        implHolder->SetTargetPath(originalTargetPath);

        auto sequoiaLinkPath = MangleSequoiaPath(originalLinkPath);
        implHolder->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(NYPath::DirNameAndBaseName(originalLinkPath).second, originalLinkPath);

        const auto& sequoiaQueueManager = GetBootstrap()->GetSequoiaQueueManager();
        auto pathToNodeIdRecord = NSequoiaClient::NRecords::TPathToNodeId{
            .Key = {.Path = sequoiaLinkPath},
            .NodeId = id.ObjectId,
        };
        sequoiaQueueManager->EnqueueWrite(pathToNodeIdRecord);
        auto nodeIdToPathRecord = NSequoiaClient::NRecords::TNodeIdToPath{
            .Key = {.NodeId = id.ObjectId},
            .Path = originalLinkPath,
        };
        sequoiaQueueManager->EnqueueWrite(nodeIdToPathRecord);

        YT_LOG_DEBUG("Link created (LinkId: %v, TargetPath: %v)",
            id,
            originalTargetPath);

        return implHolder;
    }

    void DoDestroy(TLinkNode* node) override
    {
        // TODO(aleksandra-zh, kvk1920 or somebody else): fix this when Sequoia supports branches.
        if (node->IsTrunk()) {
            const auto& cypressManager = GetBootstrap()->GetCypressManager();
            auto path = cypressManager->GetNodePath(node, {});
            const auto& sequoiaQueueManager = GetBootstrap()->GetSequoiaQueueManager();
            auto pathToNodeIdRecordKey = NSequoiaClient::NRecords::TPathToNodeIdKey{
                .Path = MangleSequoiaPath(path),
            };
            sequoiaQueueManager->EnqueueDelete(pathToNodeIdRecordKey);
            auto nodeIdToPathRecordKey = NSequoiaClient::NRecords::TNodeIdToPathKey{
                .NodeId = node->GetId(),
            };
            sequoiaQueueManager->EnqueueDelete(nodeIdToPathRecordKey);
        }
        TBase::DoDestroy(node);
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
        trunkNode->SetTargetPath(Load<NYTree::TYPath>(*context));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLinkNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TLinkNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
