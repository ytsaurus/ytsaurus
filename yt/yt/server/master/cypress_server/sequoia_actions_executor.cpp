#include "sequoia_actions_executor.h"

#include "config.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/helpers.h>

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/prerequisite_revision.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaServer;
using namespace NServer;
using namespace NTableClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaActionsExecutor
    : public ISequoiaActionsExecutor
{
public:
    explicit TSequoiaActionsExecutor(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqCreateNode, TCreateNodeActionState>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCreateNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCreateNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqAttachChild>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAttachChild, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitAttachChild, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqRemoveNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareRemoveNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitRemoveNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqDetachChild>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareDetachChild, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitDetachChild, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqSetNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitSetNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqMultisetAttributes>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitMultisetAttributes, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqCloneNode, TCloneNodeActionState>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareCloneNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitCloneNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqExplicitlyLockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitExplicitlyLockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqImplicitlyLockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareImplicitlyLockNode, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraCommitImplicitlyLockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqUnlockNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitUnlockNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqRemoveNodeAttribute>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitRemoveNodeAttribute, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqMaterializeNode>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitMaterializeNode, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqFinishNodeMaterialization>({
            .Commit = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitFinishNodeMaterialization, Unretained(this)),
        });
        transactionManager->RegisterTransactionActionHandlers<TReqValidatePrerequisiteRevisions>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaActionsExecutor::HydraPrepareAndCommitValidatePrerequisites, Unretained(this)),
        });
    }

private:
    enum class ELogStage
    {
        Preparing,
        Prepared,
        Committing,
        Committed,
        PreparingCommitting,
        PreparedCommitted
    };

    TBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    static TStringBuf FormatLogStage(ELogStage logStage)
    {
        switch (logStage) {
            case ELogStage::Preparing:
                return "Preparing";
            case ELogStage::Prepared:
                return "Prepared";
            case ELogStage::Committing:
                return "Committing";
            case ELogStage::Committed:
                return "Committed";
            case ELogStage::PreparingCommitting:
                return "Preparing and committing";
            case ELogStage::PreparedCommitted:
                return "Prepared and committed";
        }
    }

    static void VerifySequoiaNode(TCypressNode* node)
    {
        if (node->GetType() != EObjectType::Rootstock) {
            node->VerifySequoia();
        }
    }

    static bool CanCreateSequoiaType(EObjectType type)
    {
        return
            type == EObjectType::Document ||
            type == EObjectType::Orchid ||
            IsSequoiaNode(type) ||
            IsScalarType(type) ||
            IsChunkOwnerType(type);
    }

    template <CInvocable<void(TCypressNode*)> F>
    static void ForEachOriginator(
        TCypressNode* node,
        F callback) noexcept
    {
        while (true) {
            callback(node);

            VerifySequoiaNode(node);

            if (node->IsTrunk()) {
                break;
            }

            node = node->GetOriginator();
        }
    }

    static void PrepareSequoiaNodeCreation(
        TCypressNode* node,
        TNodeId parentId,
        TStringBuf key,
        TStringBuf rawPath) noexcept
    {
        auto path = TYPath(rawPath);
        ForEachOriginator(
            node,
            [&] (TCypressNode* node) {
                using TImmutableProperties = TCypressNode::TImmutableSequoiaProperties;
                using TMutableProperties = TCypressNode::TMutableSequoiaProperties;

                node->ImmutableSequoiaProperties() = std::make_unique<TImmutableProperties>(TImmutableProperties(
                    std::string(key),
                    path,
                    parentId));
                node->MutableSequoiaProperties() = std::make_unique<TMutableProperties>(TMutableProperties{
                    .BeingCreated = true,
                });
            });
    }

    void CommitSequoiaNodeCreation(const TCypressNodePtr& node)
    {
        auto* currentNode = node.Get();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->GetHandler(currentNode);
        handler->SetReachable(currentNode);

        ForEachOriginator(
            currentNode,
            [] (TCypressNode* node) {
                YT_VERIFY(std::exchange(node->MutableSequoiaProperties()->BeingCreated, false));
            });
    }

    struct TCreateNodeActionState
    {
        TCypressNodePtr Node;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Node);
        }
    };

    static void CreateSequoiaPropertiesForMaterializedNode(TCypressNode* node) noexcept
    {
        ForEachOriginator(
            node,
            [] (TCypressNode* node) {
                node->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>(
                    TCypressNode::TMutableSequoiaProperties{
                        .BeingCreated = true,
                    });
            });
    }

    void FinishSequoiaNodeMaterialization(TCypressNode* node, TNodeId parentId, const TYPath& path)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->GetHandler(node);
        handler->SetReachable(node);

        ForEachOriginator(
            node,
            [&] (TCypressNode* node) {
                node->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(
                    TCypressNode::TImmutableSequoiaProperties(
                        NYPath::DirNameAndBaseName(path).second,
                        path,
                        parentId));

                node->MutableSequoiaProperties()->BeingCreated = false;
            });
    }

    // NB: Sequoia node has to be created in prepare phase since we cannot
    // guarantee that object id will not be used between prepare and commit.
    // It's ok because this node cannot be reached from any other node until
    // Sequoia tx is committed: AttachChild for created subtree's root is
    // executed via "late prepare" after prepares on all participants are
    // succeeded.
    // TODO(kvk1920): implement proper UnstageNode() and use it here.
    // See YT-14219.
    void HydraPrepareCreateNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqCreateNode* request,
        TCreateNodeActionState* state,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId(), sequoiaTransaction->GetAuthenticationIdentity());

        auto type = FromProto<EObjectType>(request->type());
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& path = request->path();

        auto explicitAttributes = request->has_node_attributes()
            ? FromProto(request->node_attributes())
            : EmptyAttributes().Clone();

        auto inheritedAttributes = FromProto(request->inherited_attributes());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto accountName = inheritedAttributes->GetAndRemove<std::string>(NServer::EInternedAttributeKey::Account.Unintern());
        auto optionalAccount = explicitAttributes->FindAndRemove<std::string>(NServer::EInternedAttributeKey::Account.Unintern());
        auto* account = securityManager->GetAccountByNameOrThrow(optionalAccount.value_or(accountName), /*activeLifeStageOnly*/ true);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto roles = multicellManager->GetMasterCellRoles(multicellManager->GetCellTag());
        if (None(roles & EMasterCellRoles::SequoiaNodeHost) && IsSequoiaId(nodeId)) {
            THROW_ERROR_EXCEPTION("This cell cannot host Sequoia nodes");
        }

        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            cypressTransaction,
            account,
            /*options*/ {});

        Y_UNUSED(nodeFactory->CreateNode(
            type,
            nodeId,
            inheritedAttributes.Get(),
            explicitAttributes.Get()));
        auto* node = cypressManager->GetNode({nodeId, cypressTransactionId});
        // This takes a strong reference for the newly-created node.
        state->Node.Assign(node);

        PrepareSequoiaNodeCreation(
            node,
            parentId,
            /*key*/ NYPath::DirNameAndBaseName(path).second,
            /*path*/ request->path());

        nodeFactory->Commit();

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId(), sequoiaTransaction->GetAuthenticationIdentity());
    }

    void HydraCommitCreateNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqCreateNode* request,
        TCreateNodeActionState* state,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId(), sequoiaTransaction->GetAuthenticationIdentity());

        CommitSequoiaNodeCreation(state->Node);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId(), sequoiaTransaction->GetAuthenticationIdentity());
    }

    void DoLog(const NProto::TReqCreateNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId, const NRpc::TAuthenticationIdentity& auth)
    {
        auto type = FromProto<EObjectType>(request.type());
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto parentId = FromProto<TNodeId>(request.parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        const auto& path = request.path();
        IAttributeDictionaryPtr explicitAttributes;
        if (request.has_node_attributes()) {
            explicitAttributes = FromProto(request.node_attributes());
        }

        YT_LOG_DEBUG("%v CreateNode "
            "(SequoiaTransactionId: %v, Type: %v, NodeId: %v, Path: %v, ParentNodeId: %v, Attributes: %v, CypressTransactionId: %v, Auth: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            type,
            nodeId,
            path,
            parentId,
            MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
                if (explicitAttributes) {
                    builder->AppendString(ConvertToYsonString(*explicitAttributes, EYsonFormat::Text).AsStringBuf());
                } else {
                    builder->AppendString("<null>");
                }
            }),
            cypressTransactionId,
            auth);
    }

    void HydraPrepareAttachChild(
        TTransaction* sequoiaTransaction,
        NProto::TReqAttachChild* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& key = request->key();

        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId());

        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->CypressManager;

        ValidateYTreeKey(key, config->MaxMapNodeKeyLength);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* parent = cypressManager->GetNodeOrThrow(TVersionedNodeId(parentId));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        VerifySequoiaNode(parent);
        auto beingCreated = parent->MutableSequoiaProperties()->BeingCreated;
        THROW_ERROR_EXCEPTION_IF(
            !beingCreated && !options.LatePrepare,
            "Operation is not atomic for user");

        // NB: ImmutableSequoiaProperties could be null in some cases, e.g. when a subtree is being
        // copied to Sequoia. In such a case we suppress the check as it seems unimportant.
        // See also YT-26439.
        if (parent->GetType() == EObjectType::SequoiaMapNode && parent->ImmutableSequoiaProperties()) {
            int childCount = NCypressServer::GetNodeChildCount(
                parent->As<TSequoiaMapNode>(),
                cypressTransaction);
            int maxChildCount = config->MaxNodeChildCount;
            ValidateYTreeChildCount(
                parent->ImmutableSequoiaProperties()->Path,
                childCount,
                maxChildCount);
        }

        if (options.LatePrepare) {
            cypressManager->LockNode(
                parent,
                cypressTransaction,
                TLockRequest::MakeSharedChild(key));
        } else {
            YT_LOG_ALERT_AND_THROW_UNLESS(
                parent->MutableSequoiaProperties()->BeingCreated,
                "An attempt to attach a child in non late prepare mode was made, despite the fact that "
                "parent node is not marked as BeingCreated (ParentId: %v, TransactionId: %v, ChildKey: %v)",
                parentId,
                cypressTransactionId,
                key);
        }

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId());
    }

    void HydraCommitAttachChild(
        TTransaction* sequoiaTransaction,
        NProto::TReqAttachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        YT_VERIFY(
            TypeFromId(parentId) == EObjectType::SequoiaMapNode ||
            TypeFromId(parentId) == EObjectType::Scion);
        auto childId = FromProto<TNodeId>(request->child_id());
        const auto& key = request->key();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& accessTrackingOptions = request->access_tracking_options();

        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        auto* trunkParent = cypressManager->GetNode(TVersionedNodeId(parentId))->As<TSequoiaMapNode>();
        if (!IsObjectAlive(trunkParent)) {
            YT_LOG_ALERT("An attempt to attach a child to a zombie Sequoia node was made "
                "(ParentId: %v, ChildId: %v, ChildKey: %v)",
                parentId,
                childId,
                key);
        }

        // NB: No-op in case of late prepare.
        auto* parent = cypressManager->LockNode(
            trunkParent,
            cypressTransaction,
            TLockRequest::MakeSharedChild(key));

        VerifySequoiaNode(parent);
        AttachChildToSequoiaNodeOrThrow(parent, key, childId);

        MaybeTouchNode(
            cypressManager,
            parent,
            EModificationType::Content,
            accessTrackingOptions);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqAttachChild& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto parentId = FromProto<TNodeId>(request.parent_id());
        auto childId = FromProto<TNodeId>(request.child_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        const auto& key = request.key();
        const auto& accessTrackingOptions = request.access_tracking_options();

        YT_LOG_DEBUG("%v AttachChild "
            "(SequoiaTransactionId: %v, ChildNodeId: %v, ParentNodeId: %v, Key: %v, CypressTransactionId: %v, %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            childId,
            parentId,
            key,
            cypressTransactionId,
            accessTrackingOptions);
    }

    void HydraPrepareDetachChild(
        TTransaction* sequoiaTransaction,
        NProto::TReqDetachChild* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& key = request->key();

        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(parentId));
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto* node = cypressManager
            ->GetVersionedNode(trunkNode, cypressTransaction)
            ->As<TSequoiaMapNode>();
        while (!node->IsTrunk() && !node->KeyToChild().contains(key)) {
            node = node->GetOriginator()->As<TSequoiaMapNode>();
        }

        if (!GetOrDefault(node->KeyToChild(), key, NullObjectId)) {
            THROW_ERROR_EXCEPTION("Node %v does not has a child with key %Qv",
                parentId,
                key);
        }

        cypressManager->CheckLock(
            trunkNode,
            cypressTransaction,
            TLockRequest::MakeSharedChild(key))
            .ThrowOnError();

        // NB: Nobody can acquire the shared child lock for this node between
        // prepare and commit due to Sequoia table lock. DetachChild acquires
        // exclusive lock on (nodeId, topmostTx, key) in "child_nodes" Sequoia
        // table.

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId());
    }

    void HydraCommitDetachChild(
        TTransaction* sequoiaTransaction,
        NProto::TReqDetachChild* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto parentId = FromProto<TNodeId>(request->parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& key = request->key();
        const auto& accessTrackingOptions = request->access_tracking_options();

        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* trunkParent = cypressManager->GetNode(TVersionedNodeId(parentId))->As<TSequoiaMapNode>();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        if (!FindMapNodeChild(cypressManager, trunkParent, cypressTransaction, key)) {
            YT_LOG_ALERT("Sequoia map node has no such child (ParentId: %v, Key: %v)",
                parentId,
                key);
            return;
        }

        auto* parent = cypressManager->LockNode(
            trunkParent,
            cypressTransaction,
            TLockRequest::MakeSharedChild(key))
            ->As<TSequoiaMapNode>();

        auto& children = parent->MutableChildren();
        if (parent->IsTrunk()) {
            auto childIt = children.KeyToChild().find(key);
            children.Remove(key, childIt->second);
        } else {
            children.Set(key, NullObjectId);
        }

        --parent->ChildCountDelta();

        MaybeTouchNode(
            cypressManager,
            parent,
            EModificationType::Content,
            accessTrackingOptions);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqDetachChild& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto parentId = FromProto<TNodeId>(request.parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        const auto& key = request.key();
        const auto& accessTrackingOptions = request.access_tracking_options();

        YT_LOG_DEBUG("%v DetachChild "
            "(SequoiaTransactionId: %v, ParentNodeId: %v, Key: %v, CypressTransactionId: %v, %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            parentId,
            key,
            cypressTransactionId,
            accessTrackingOptions);
    }

    void HydraPrepareRemoveNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqRemoveNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        TTransaction* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        if (TypeFromId(nodeId) == EObjectType::Rootstock && cypressTransaction) {
            THROW_ERROR_EXCEPTION("Rootstock cannot be removed under transaction");
        }

        cypressManager
            ->CheckLock(trunkNode, cypressTransaction, ELockMode::Exclusive)
            .ThrowOnError();

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId());
    }

    void HydraCommitRemoveNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqRemoveNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNode(TVersionedObjectId(nodeId));
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        if (!IsObjectAlive(trunkNode)) {
            YT_LOG_ALERT(
                "Attempted to remove unexisting Sequoia node; ignored "
                "(NodeId: %v)",
                nodeId);
            return;
        }

        VerifySequoiaNode(trunkNode);

        if (trunkNode->GetType() == EObjectType::Rootstock) {
            auto parentNode = trunkNode->GetParent()->As<TCypressMapNode>();
            if (!parentNode) {
                YT_LOG_ALERT(
                    "Attempted to remove rootstock that is already detached from a parent, ignored "
                    "(RootstockNodeId: %v)",
                    nodeId);
                return;
            }
            auto parentProxy = cypressManager->GetNodeProxy(parentNode);
            parentProxy->AsMap()->RemoveChild(cypressManager->GetNodeProxy(trunkNode));
            return;
        }


        auto* node = trunkNode;
        // NB: lock is already checked in prepare. Nobody cannot lock this node
        // between prepare and commit of Sequoia tx due to:
        //   - exlusive lock in "node_id_to_path" Sequoia table;
        //   - barrier for Sequoia tx.
        if (cypressTransaction) {
            auto* branchedNode = cypressManager->LockNode(trunkNode, cypressTransaction, ELockMode::Exclusive);
            branchedNode->MutableSequoiaProperties()->Tombstone = true;
            node = branchedNode;
        }

        const auto& handler = cypressManager->GetHandler(trunkNode);
        handler->SetUnreachable(node);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqRemoveNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());

        YT_LOG_DEBUG("%v RemoveNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId);
    }

    void HydraPrepareAndCommitSetNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqSetNode* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& path = request->path();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto force = request->force();
        const auto& accessTrackingOptions = request->access_tracking_options();

        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto innerRequest = TCypressYPathProxy::Set(path);
        innerRequest->set_value(request->value());
        innerRequest->set_force(force);
        SetAccessTrackingOptions(innerRequest, accessTrackingOptions);
        if (request->has_effective_acl()) {
            YT_LOG_ALERT_IF(path.empty(),
                "Effective ACL was provided with empty relative path (NodeId: %v)",
                nodeId);
            SetSequoiaNodeEffectiveAcl(&innerRequest->Header(), request->effective_acl());
        }

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqSetNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        const auto& path = request.path();
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto force = request.force();
        const auto& accessTrackingOptions = request.access_tracking_options();

        YT_LOG_DEBUG("%v SetNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Path: %v, Force: %v, %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            path,
            force,
            accessTrackingOptions);
    }

    void HydraPrepareAndCommitMultisetAttributes(
        TTransaction* sequoiaTransaction,
        NProto::TReqMultisetAttributes* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        const auto& path = request->path();
        auto force = request->force();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& accessTrackingOptions = request->access_tracking_options();

        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        auto innerRequest = TYPathProxy::MultisetAttributes(path);
        *innerRequest->mutable_subrequests() = request->subrequests();
        innerRequest->set_force(force);
        SetAccessTrackingOptions(innerRequest, accessTrackingOptions);
        if (request->has_effective_acl()) {
            SetSequoiaNodeEffectiveAcl(&innerRequest->Header(), request->effective_acl());
        }

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqMultisetAttributes& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        const auto& path = request.path();
        auto force = request.force();
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        const auto& accessTrackingOptions = request.access_tracking_options();

        YT_LOG_DEBUG("%v MultisetAttributes "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Path: %v, SubrequestCount: %v, Force: %v, %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            path,
            request.subrequests_size(),
            force,
            accessTrackingOptions);
    }

    void HydraPrepareAndCommitRemoveNodeAttribute(
        TTransaction* sequoiaTransaction,
        NProto::TReqRemoveNodeAttribute* request,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto force = request->force();
        const auto& path = request->path();

        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to remove node attribute with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(nodeId));
        VerifySequoiaNode(trunkNode);

        if (!IsObjectAlive(trunkNode)) {
            THROW_ERROR_EXCEPTION("Cypress node %v is not alive", nodeId);
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransaction(cypressTransactionId)
            : nullptr;

        auto innerRequest = TCypressYPathProxy::Remove(path);
        innerRequest->set_force(force);
        // COPMAT(danilalexeev): Remove if.
        if (request->has_effective_acl()) {
            SetSequoiaNodeEffectiveAcl(&innerRequest->Header(), request->effective_acl());
        }

        SyncExecuteVerb(
            cypressManager->GetNodeProxy(trunkNode, cypressTransaction),
            innerRequest);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqRemoveNodeAttribute& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto force = request.force();
        const auto& path = request.path();

        YT_LOG_DEBUG("%v RemoveNodeAttribute "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Path: %v, Force: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            path,
            force);
    }

    struct TCloneNodeActionState
    {
        TWeakObjectPtr<TCypressNode> TrunkSourceNode;
        TCypressNodePtr DestinationNode;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, TrunkSourceNode);
            Persist(context, DestinationNode);
        }
    };

    // NB: See PrepareCreateNode.
    void HydraPrepareCloneNode(
        TTransaction* sequoiaTransaction,
        TReqCloneNode* request,
        TCloneNodeActionState* state,
        const TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);

        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId());

        auto sourceNodeId = FromProto<TNodeId>(request->src_id());
        auto destinationNodeId = FromProto<TNodeId>(request->dst_id());
        auto destinationParentId = FromProto<TNodeId>(request->dst_parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& destinationPath = request->dst_path();
        const auto& cloneOptions = request->options();

        // TODO(h0pless): Think about throwing an error if this cell is not sequoia_node_host anymore.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkSourceNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(sourceNodeId));
        state->TrunkSourceNode.Assign(trunkSourceNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = cypressTransactionId
            ? transactionManager->GetTransactionOrThrow(cypressTransactionId)
            : nullptr;

        // Maybe this is excessive.
        auto type = trunkSourceNode->GetType();
        if (!CanCreateSequoiaType(type)) {
            THROW_ERROR_EXCEPTION("Type %Qlv is not supported in Sequoia", type);
        }

        auto mode = FromProto<ENodeCloneMode>(cloneOptions.mode());
        TNodeFactoryOptions factoryOptions{
            .PreserveAccount = cloneOptions.preserve_account(),
            .PreserveCreationTime = cloneOptions.preserve_creation_time(),
            .PreserveModificationTime = cloneOptions.preserve_modification_time(),
            .PreserveExpirationTime = cloneOptions.preserve_expiration_time(),
            .PreserveExpirationTimeout = cloneOptions.preserve_expiration_timeout(),
            .PreserveOwner = cloneOptions.preserve_owner(),
            .PreserveAcl = cloneOptions.preserve_acl(),
            .PessimisticQuotaCheck = cloneOptions.pessimistic_quota_check(),
            .AllowSecondaryIndexAbandonment = cloneOptions.allow_secondary_index_abandonment(),
        };

        // TODO(cherepashka): after inherited attributes are supported, implement copyable-inherited attributes.
        auto inheritedAttributes = FromProto(request->inherited_attributes());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto accountName = inheritedAttributes->GetAndRemove<std::string>(NServer::EInternedAttributeKey::Account.Unintern());
        auto* account = securityManager->GetAccountByNameOrThrow(accountName, /*activeLifeStageOnly*/ true);

        auto nodeFactory = cypressManager->CreateNodeFactory(
            cypressManager->GetRootCypressShard(),
            cypressTransaction,
            account,
            factoryOptions);

        auto* sourceNode = cypressManager->GetVersionedNode(trunkSourceNode, cypressTransaction);
        auto* destinationNode = nodeFactory->CloneNode(sourceNode, mode, inheritedAttributes.Get(), destinationNodeId);
        state->DestinationNode.Assign(destinationNode);

        PrepareSequoiaNodeCreation(
            destinationNode,
            destinationParentId,
            NYPath::DirNameAndBaseName(destinationPath).second,
            destinationPath);

        nodeFactory->Commit();

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId());
    }

    void HydraCommitCloneNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqCloneNode* request,
        TCloneNodeActionState* state,
        const TTransactionCommitOptions& /*options*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId());

        // TODO(danilalexeev): Sequoia transaction has to lock |request->src_id()| to conflict
        // with an expired nodes removal.
        if (auto* trunkSourceNode = state->TrunkSourceNode.Get(); IsObjectAlive(trunkSourceNode)) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            MaybeTouchNode(cypressManager, trunkSourceNode);
        }

        CommitSequoiaNodeCreation(state->DestinationNode);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId());
    }

    void DoLog(const TReqCloneNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto sourceNodeId = FromProto<TNodeId>(request.src_id());
        auto destinationNodeId = FromProto<TNodeId>(request.dst_id());
        auto destinationParentId = FromProto<TNodeId>(request.dst_parent_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        const auto& destinationPath = request.dst_path();
        const auto& cloneOptions = request.options();

        YT_LOG_DEBUG("%v CloneNode "
            "(SequoiaTransactionId: %v, SourceNodeId: %v, CypressTransactionId: %v, DestinationNodeId: %v, DestinationPath: %v, DestinationParentId: %v, Options: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            sourceNodeId,
            cypressTransactionId,
            destinationNodeId,
            destinationPath,
            destinationParentId,
            cloneOptions);
    }

    void HydraPrepareAndCommitExplicitlyLockNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqExplicitlyLockNode* request,
        const TTransactionPrepareOptions& options)
    {
        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);
        auto timestamp = request->timestamp();
        auto waitable = request->waitable();
        auto lockId = FromProto<TLockId>(request->lock_id());

        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to explicitly lock node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to explicitly lock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            CheckLockRequest(mode, childKey, attributeKey).IsOK(),
            "Explicit lock request is malformed "
            "(NodeId: %v, TransactionId: %v, Mode: %v, ChildKey: %v, AttributeKey: %v)",
            nodeId,
            cypressTransactionId,
            mode,
            childKey,
            attributeKey);

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        auto lockRequest = CreateLockRequest(mode, childKey, attributeKey, timestamp);
        proxy->Lock(lockRequest, waitable, lockId);

        MaybeTouchNode(cypressManager, trunkNode);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqExplicitlyLockNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto mode = FromProto<ELockMode>(request.mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(request, attribute_key);
        auto timestamp = request.timestamp();
        auto waitable = request.waitable();
        auto lockId = FromProto<TLockId>(request.lock_id());

        YT_LOG_DEBUG("%v ExplicitlyLockNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Mode: %v, ChildKey: %v, AttributeKey: %v, Timestamp: %v, Waitable: %v, LockId: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            mode,
            childKey,
            attributeKey,
            timestamp,
            waitable,
            lockId);
    }


    void HydraPrepareImplicitlyLockNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqImplicitlyLockNode* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        DoLog(*request, ELogStage::Preparing, sequoiaTransaction->GetId());

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to implicitly lock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);
        auto lockRequest = CreateLockRequest(
            mode,
            childKey,
            attributeKey,
            /*timestamp*/ 0);
        cypressManager->CheckLock(trunkNode, cypressTransaction, lockRequest).ThrowOnError();

        DoLog(*request, ELogStage::Prepared, sequoiaTransaction->GetId());
    }

    void HydraCommitImplicitlyLockNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqImplicitlyLockNode* request,
        const TTransactionCommitOptions& /*options*/)
    {
        DoLog(*request, ELogStage::Committing, sequoiaTransaction->GetId());

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mode = FromProto<ELockMode>(request->mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* trunkNode = cypressManager->GetNode(TVersionedNodeId(nodeId));
        auto* cypressTransaction = transactionManager->GetTransaction(cypressTransactionId);

        auto lockRequest = CreateLockRequest(
            mode,
            childKey,
            attributeKey,
            /*timestamp*/ 0);
        cypressManager->LockNode(trunkNode, cypressTransaction, lockRequest);

        DoLog(*request, ELogStage::Committed, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqImplicitlyLockNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto mode = FromProto<ELockMode>(request.mode());
        auto childKey = YT_OPTIONAL_FROM_PROTO(request, child_key);
        auto attributeKey = YT_OPTIONAL_FROM_PROTO(request, attribute_key);

        YT_LOG_DEBUG("%v ImplicitlyLockNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Mode: %v, ChildKey: %v, AttributeKey: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            mode,
            childKey,
            attributeKey);
    }

    void HydraPrepareAndCommitUnlockNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqUnlockNode* request,
        const TTransactionPrepareOptions& options)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());

        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to unlock node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to unlock node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedNodeId(nodeId));
        VerifySequoiaNode(trunkNode);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransaction(cypressTransactionId);

        auto proxy = cypressManager->GetNodeProxy(trunkNode, cypressTransaction);
        proxy->Unlock();

        MaybeTouchNode(cypressManager, trunkNode);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqUnlockNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());

        YT_LOG_DEBUG("%v UnlockNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId);
    }

    void HydraPrepareAndCommitMaterializeNode(
        TTransaction* sequoiaTransaction,
        NProto::TReqMaterializeNode* request,
        const TTransactionPrepareOptions& options)
    {
        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto version = request->version();
        auto mode = FromProto<ENodeCloneMode>(request->mode());
        auto existingNodeId = FromProto<TNodeId>(request->existing_node_id());
        auto newAccountId = request->has_new_account_id()
            ? std::make_optional(FromProto<TAccountId>(request->new_account_id()))
            : std::nullopt;
        auto preserveCreationTime = request->preserve_creation_time();
        auto preserveExpirationTime = request->preserve_expiration_time();
        auto preserveExpirationTimeout = request->preserve_expiration_timeout();
        auto preserveOwner = request->preserve_owner();
        auto inheritedAttributes = request->has_inherited_attributes_override()
            ? FromProto(request->inherited_attributes_override())
            : New<TInheritedAttributeDictionary>(Bootstrap_);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            options.LatePrepare,
            "An attempt to materialize node with non late prepare mode was made (NodeId: %v, TransactionId: %v)",
            nodeId,
            cypressTransactionId);

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to materialize node with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);

        if (version != NCellMaster::GetCurrentReign()) {
            THROW_ERROR_EXCEPTION("Invalid node metadata format version: expected %v, actual %v",
                NCellMaster::GetCurrentReign(),
                version);
        }

        TAccount* account = nullptr;
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        // Presence of a new account means that user does not want to preserve an old one.
        if (newAccountId) {
            account = securityManager->GetAccountOrThrow(*newAccountId, /*activeLifeStageOnly*/ true);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto factory = cypressManager->CreateNodeFactory(
            /*shard*/ nullptr, // Shard will be set when assembling the tree.
            cypressTransaction,
            account,
            TNodeFactoryOptions{
                .PreserveAccount = !newAccountId.has_value(),
                .PreserveCreationTime = preserveCreationTime,
                .PreserveModificationTime = true, // Modification time will be changed when assembling subtree, if needed.
                .PreserveExpirationTime = preserveExpirationTime,
                .PreserveExpirationTimeout = preserveExpirationTimeout,
                .PreserveOwner = preserveOwner,
                .PreserveAcl = true, // Same as modification time.
                .PessimisticQuotaCheck = false // This is checked on the cypress proxy.
            },
            /*serviceTrunkNode*/ nullptr,
            /*unresolvedSuffix*/ {}); // Unused during copy, relevant only for "create".

        const auto& serializedNode = request->serialized_node();
        TMaterializeNodeContext copyContext(
            Bootstrap_,
            mode,
            TRef::FromString(serializedNode.data()),
            nodeId,
            /*materializeAsSequoiaNode*/ true,
            FromProto<TMasterTableSchemaId>(serializedNode.schema_id()),
            existingNodeId);

        auto* node = factory->MaterializeNode(inheritedAttributes.Get(), &copyContext);

        // It's important to respect the invariant that each Sequoia node has mutable properties initialized.
        CreateSequoiaPropertiesForMaterializedNode(node);

        // TODO(h0pless): When expanding list of inherited attributes re-calculated during copy, some trivial
        // setter code should be written somewhere here.
        // Since only chunk_merger_mode is supported right now it's fine to leave it as-is.
        factory->Commit();

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqMaterializeNode& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto version = request.version();
        auto mode = FromProto<ENodeCloneMode>(request.mode());
        auto existingNodeId = FromProto<TNodeId>(request.existing_node_id());
        auto newAccountId = request.has_new_account_id()
            ? std::make_optional(FromProto<TAccountId>(request.new_account_id()))
            : std::nullopt;
        auto preserveCreationTime = request.preserve_creation_time();
        auto preserveExpirationTime = request.preserve_expiration_time();
        auto preserveExpirationTimeout = request.preserve_expiration_timeout();
        auto preserveOwner = request.preserve_owner();
        auto inheritedAttributes = request.has_inherited_attributes_override()
            ? FromProto(request.inherited_attributes_override())
            : New<TInheritedAttributeDictionary>(Bootstrap_);

        YT_LOG_DEBUG("%v MaterializeNode "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, Version: %v, Mode: %v, ExistingNodeId: %v, "
            "SerializedNodeSize: %v, NewAccountId: %v, PreserveCreationTime: %v, PreserveExpirationTime: %v, "
            "PreserveExpirationTimeout: %v, PreserveOwner: %v, InheritedAttributesOverride: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            version,
            mode,
            existingNodeId,
            request.serialized_node().data().size(),
            newAccountId,
            preserveCreationTime,
            preserveExpirationTime,
            preserveExpirationTimeout,
            preserveOwner,
            ConvertToYsonString(*inheritedAttributes, EYsonFormat::Text));
    }

    /*
     * This function changes BeingCreated without late prepare option.
     * This can go bad because of two things:
     * 1. User could read this mostly-but-not-quite created node before this function.
     *    This is not possible because Cypress to Sequoia copy requests are always executed
     *    under a transaction. Said transaction is internal, and client does not know its ID.
     *    Additionally, the copy process involves creation of new nodes under aforementioned
     *    transaction, which makes it impossible for client to access before commit.
     *
     * 2. Currently Sequoia session replies to user when all participants are prepared,
     *    but before they commit. This is also not possible.
     *    See TTransactionSupervisor::WaitUntilPreparedTransactionsFinished().
     */
    void HydraPrepareAndCommitFinishNodeMaterialization(
        TTransaction* sequoiaTransaction,
        NProto::TReqFinishNodeMaterialization* request,
        const TTransactionCommitOptions& /*options*/)
    {
        DoLog(*request, ELogStage::PreparingCommitting, sequoiaTransaction->GetId());

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto parentId = FromProto<TNodeId>(request->parent_id());
        const auto& path = request->path();
        auto cypressTransactionId = FromProto<TTransactionId>(request->transaction_id());
        auto preserveAcl = request->preserve_acl();
        auto preserveModificationTime = request->preserve_modification_time();

        YT_LOG_ALERT_AND_THROW_UNLESS(
            cypressTransactionId,
            "Received a request to finish node materialization with unspecified Cypress transaction (NodeId: %v)",
            nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* cypressTransaction = transactionManager->GetTransactionOrThrow(cypressTransactionId);
        auto* trunkNode = cypressManager->GetNode(TVersionedNodeId(nodeId));
        auto* node = cypressManager->GetVersionedNode(trunkNode, cypressTransaction);
        if (!preserveAcl) {
            // Acls are always preserved during materialization.
            trunkNode->Acd().ClearEntries();
        }

        if (!preserveModificationTime) {
            // Using this to avoid writing "Revised" to the access log.
            // It's actually probably fine to just use cypressManager->SetModified.
            node->SetModified(EModificationType::Content);
        }

        FinishSequoiaNodeMaterialization(node, parentId, path);

        DoLog(*request, ELogStage::PreparedCommitted, sequoiaTransaction->GetId());
    }

    void DoLog(const NProto::TReqFinishNodeMaterialization& request, ELogStage logStage, TTransactionId sequoiaTransactionId)
    {
        auto nodeId = FromProto<TNodeId>(request.node_id());
        auto parentId = FromProto<TNodeId>(request.parent_id());
        const auto& path = request.path();
        auto cypressTransactionId = FromProto<TTransactionId>(request.transaction_id());
        auto preserveAcl = request.preserve_acl();
        auto preserveModificationTime = request.preserve_modification_time();

        YT_LOG_DEBUG("%v FinishNodeMaterialization "
            "(SequoiaTransactionId: %v, NodeId: %v, CypressTransactionId: %v, ParentNodeId: %v, Path: %v, PreserveModificationTime: %v, PreserveAcl: %v)",
            FormatLogStage(logStage),
            sequoiaTransactionId,
            nodeId,
            cypressTransactionId,
            parentId,
            path,
            preserveModificationTime,
            preserveAcl);
    }

    //! NB: Cypress proxy acquires locks in dynamic tables for request target and additional paths,
    // and we do not support prerequisite revisions different from those paths.
    void HydraPrepareAndCommitValidatePrerequisites(
        TTransaction* /*transaction*/,
        NProto::TReqValidatePrerequisiteRevisions* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto revisions = FromProto<std::vector<NSequoiaClient::TResolvedPrerequisiteRevision>>(request->prerequisite_revisions());

        for (const auto& revision : revisions) {
            YT_LOG_ALERT_IF(
                CellTagFromId(revision.NodeId) != multicellManager->GetCellTag(),
                "Node from foreign cell occured in prerequisite revisions (NodeId: %v, CellTag: %v)",
                revision.NodeId,
                multicellManager->GetCellTag());

            auto* trunkNode = cypressManager->GetNodeOrThrow(TVersionedObjectId(revision.NodeId));
            if (trunkNode->GetRevision() != revision.Revision) {
                THROW_ERROR_EXCEPTION(
                    NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: node %v revision mismatch: expected %x, found %x",
                    revision.NodePath,
                    revision.Revision,
                    trunkNode->GetRevision());
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
