#include "node_proxy.h"

#include "private.h"
#include "bootstrap.h"
#include "path_resolver.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/reverse_resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/children_nodes.record.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public TYPathServiceBase
{
public:
    TNodeProxyBase(
        IBootstrap* bootstrap,
        TObjectId id,
        TYPath path,
        ISequoiaTransactionPtr transaction)
        : Bootstrap_(bootstrap)
        , Id_(id)
        , Path_(std::move(path))
        , Transaction_(std::move(transaction))
    { }

    virtual void ValidateType() const = 0;

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        // NB: In most cases resolve should be performed by sequoia service.

        return TResolveResultHere{path};
    }

protected:
    IBootstrap* const Bootstrap_;
    // TODO(kvk1920): Since `TPathResolver` tries to resolve node's ancestors
    // too we already known their ids. Ancestors' ids could be passed to the
    // constructor in order to reduce lookup count when ancestors are needed.
    const TObjectId Id_;
    const TYPath Path_;
    const ISequoiaTransactionPtr Transaction_;

    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, Get);
    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, Remove);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Remove);

        return TYPathServiceBase::DoInvoke(context);
    }

    TCellId CellIdFromCellTag(TCellTag cellTag) const
    {
        return Bootstrap_->GetNativeConnection()->GetMasterCellId(cellTag);
    }

    TCellId CellIdFromObjectId(TObjectId id) const
    {
        return CellIdFromCellTag(CellTagFromId(id));
    }

    TObjectServiceProxy CreateReadProxyForObject(TObjectId id)
    {
        return CreateObjectServiceReadProxy(
            Bootstrap_->GetNativeClient(),
            EMasterChannelKind::Follower,
            CellTagFromId(id),
            Bootstrap_->GetNativeConnection()->GetStickyGroupSizeCache());
    }

    void RemoveNode(
        TNodeId nodeId,
        const TMangledSequoiaPath& path)
    {
        YT_VERIFY(TypeFromId(nodeId) != EObjectType::Rootstock);

        auto [parentPath, childKey] = DirNameAndBaseName(DemangleSequoiaPath(path));

        // Remove from resolve table.
        Transaction_->DeleteRow(NRecords::TResolveNodeKey{
            .Path = path,
        });

        // Remove from reverse resolve table.
        Transaction_->DeleteRow(NRecords::TReverseResolveNodeKey{
            .NodeId = nodeId,
        });

        // Remove from children nodes table;
        Transaction_->DeleteRow(NRecords::TChildrenNodesKey{
            .ParentPath = MangleSequoiaPath(parentPath),
            .ChildKey = childKey,
        });

        // Remove from master cell.
        NCypressServer::NProto::TReqRemoveNode reqRemoveNode;
        ToProto(reqRemoveNode.mutable_node_id(), nodeId);
        Transaction_->AddTransactionAction(
            CellTagFromId(nodeId),
            MakeTransactionActionData(reqRemoveNode));
    }

    TCellTag RemoveRootstock()
    {
        YT_VERIFY(TypeFromId(Id_) == EObjectType::Scion);

        // Scion removal causes rootstock removal.
        // Since rootstock's parent _always_ lives at the same cell as rootstock
        // `DetachChild()` isn't needed.

        // TODO(kvk1920): Think about inferring rootstock's id from scion's one.
        auto reqGet = TYPathProxy::Get(FromObjectId(Id_) + "/@rootstock_id");
        auto rspGet = WaitFor(CreateReadProxyForObject(Id_).Execute(reqGet))
            .ValueOrThrow();
        auto rootstockId = ConvertTo<TNodeId>(NYson::TYsonString(rspGet->value()));

        NCypressServer::NProto::TReqRemoveNode reqRemoveRootstock;
        ToProto(reqRemoveRootstock.mutable_node_id(), rootstockId);
        Transaction_->AddTransactionAction(
            CellTagFromId(rootstockId),
            MakeTransactionActionData(reqRemoveRootstock));
        return CellTagFromId(rootstockId);
    }

    void VerifyRemovalOfNodeWithNonSequoiaParent(const TYPath& parentPath)
    {
        // Only scion node can have non-Sequoia parent.
        if (TypeFromId(Id_) != EObjectType::Scion) {
            YT_LOG_FATAL("Attempted to remove Sequoia node with non-Sequoia parent (ParentPath: %v, NodePath: %v, NodeId: %v)",
                parentPath,
                Path_,
                Id_);
        }
    }

    void DetachThisFromParent(TNodeId parentId, const TString& thisKey)
    {
        NCypressServer::NProto::TReqDetachChild reqDetachChild;
        ToProto(reqDetachChild.mutable_parent_id(), parentId);
        reqDetachChild.set_key(thisKey);
        Transaction_->AddTransactionAction(
            CellTagFromId(parentId),
            MakeTransactionActionData(reqDetachChild));
    }

    void HandleUnresolvedSuffixOnRemoval(
        const TCtxRemovePtr& context,
        const TReqRemove* request,
        TRspRemove* response,
        TYPathBuf unresolvedSuffix)
    {
        YT_VERIFY(!unresolvedSuffix.empty());

        // Try to reproduce Cypress behavior.

        auto type = TypeFromId(Id_);
        // TODO(kvk1920): Support documents.
        if (!IsSequoiaCompositeNodeType(type)) {
            THROW_ERROR_EXCEPTION("Node %v cannot have children", Path_);
        }

        TTokenizer tokenizer(unresolvedSuffix);
        tokenizer.Advance();

        if (tokenizer.GetType() == ETokenType::At) {
            // Just redirect to an appropriate master cell.
            auto newRequest = TYPathProxy::Remove(Path_ + unresolvedSuffix);
            newRequest->CopyFrom(*request);
            SetRequestTargetYPath(&newRequest->Header(), FromObjectId(Id_) + unresolvedSuffix);

            auto objectWriteProxy = CreateObjectServiceWriteProxy(
                Bootstrap_->GetNativeClient(),
                CellTagFromId(Id_));

            auto masterResponse = WaitFor(objectWriteProxy.Execute(std::move(newRequest)))
                .ValueOrThrow();
            response->CopyFrom(*masterResponse);
            context->Reply();
            return;
        }

        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);

        // There is no composite node type other than sequoia map node. If we
        // have unresolved suffix it can be either attribute or non-existent child.
        THROW_ERROR_EXCEPTION(
            "Node %v has no child with key %Qv",
            Path_,
            ToYPathLiteral(tokenizer.GetToken()));
    }
};

DEFINE_YPATH_SERVICE_METHOD(TNodeProxyBase, Get)
{
    // TODO(kvk1920): Optimize.
    // NB: There are 3 kinds of attributes:
    //  1. Already known (path, id);
    //  2. Replicated to Sequoia tables;
    //  3. Master-only.
    // If request contains at least 1 attribute of 3d kind we can just redirect
    // whole request to master.

    // TODO(kvk1920): In case of race between Get(path) and Create(path, force=true)
    // for the same path we can get an error "no such node".
    // Retry is needed if a given path still exists.

    // TODO(kvk1920): Generalize request redirection.
    auto suffix = GetRequestTargetYPath(context->GetRequestHeader());

    auto newRequest = TYPathProxy::Get();
    newRequest->CopyFrom(*request);

    SetRequestTargetYPath(&newRequest->Header(), FromObjectId(Id_) + suffix);

    auto objectServiceReadProxy = CreateReadProxyForObject(Id_);
    auto rsp = WaitFor(objectServiceReadProxy.Execute(std::move(newRequest)))
        .ValueOrThrow();
    response->CopyFrom(*rsp);
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxyBase, Remove)
{
    if (auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader()); !unresolvedSuffix.empty()) {
        HandleUnresolvedSuffixOnRemoval(context, request, response, unresolvedSuffix);
        return;
    }

    if (request->force()) {
        // TODO(kvk1920): Current cypress behaviour is just to ignore some errors.
        THROW_ERROR_EXCEPTION("Remove with \"force\" flag is not supported in Sequoia yet");
    }

    auto [parentPath, thisName] = DirNameAndBaseName(Path_);

    // Acquire shared lock on parent node.
    Transaction_->LockRow(
        NRecords::TResolveNodeKey{.Path = MangleSequoiaPath(parentPath)},
        ELockType::SharedStrong);

    auto parent = ResolvePath(Transaction_, parentPath);
    // Ensure that every case is handled.
    static_assert(std::variant_size<decltype(parent)>() == 2);

    TCellTag subtreeRootCell;
    if (std::holds_alternative<TCypressResolveResult>(parent)) {
        // Only scion node can have non-Sequoia parent.
        VerifyRemovalOfNodeWithNonSequoiaParent(parentPath);
        subtreeRootCell = RemoveRootstock();
    } else {
        auto parentId = GetOrCrash<TSequoiaResolveResult>(parent).ResolvedPrefixNodeId;
        DetachThisFromParent(parentId, thisName);
        subtreeRootCell = CellTagFromId(parentId);
    }

    auto mangledPath = MangleSequoiaPath(Path_);

    // NB: For non-recursive removal we have to check if directory is empty.
    // This can be done via requesting just 2 rows.
    auto selectedRowsLimit = request->recursive() ? std::nullopt : std::optional(2);

    auto nodesToRemove = WaitFor(Transaction_->SelectRows<NRecords::TResolveNodeKey>(
        {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath)),
        },
        selectedRowsLimit))
        .ValueOrThrow();
    YT_VERIFY(nodesToRemove.size() >= 1);

    if (!request->recursive() && nodesToRemove.size() > 1) {
        THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
    }

    for (const auto& node : nodesToRemove) {
        RemoveNode(node.NodeId, node.Key.Path);
    }

    WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromCellTag(subtreeRootCell),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
        .ThrowOnError();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TMapLikeNodeProxy
    : public TNodeProxyBase
{
public:
    using TNodeProxyBase::TNodeProxyBase;

    void ValidateType() const override
    {
        auto type = TypeFromId(Id_);

        if (!IsSupportedSequoiaType(type)) {
            THROW_ERROR_EXCEPTION(
                "Object type %Qlv is not supported in Sequoia yet",
                type);
        }
    }

private:
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, List);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TNodeProxyBase::DoInvoke(context);
    }

    TCellId GetObjectCellId(TObjectId id)
    {
        return Bootstrap_->GetNativeConnection()->GetMasterCellId(CellTagFromId(id));
    }

    void CreateNode(EObjectType type, TObjectId id, const TYPath& path)
    {
        auto [parentPath, childKey] = DirNameAndBaseName(path);
        Transaction_->WriteRow(NRecords::TResolveNode{
            .Key = {.Path = MangleSequoiaPath(path)},
            .NodeId = id,
        });
        Transaction_->WriteRow(NRecords::TReverseResolveNode{
            .Key = {.NodeId = id},
            .Path = path,
        });
        Transaction_->WriteRow(NRecords::TChildrenNodes{
            .Key = {
                .ParentPath = MangleSequoiaPath(parentPath),
                .ChildKey = TString(childKey),
            },
            .ChildId = id,
        });

        NCypressServer::NProto::TReqCreateNode createNodeRequest;
        createNodeRequest.set_type(ToProto<int>(type));
        ToProto(createNodeRequest.mutable_node_id(), id);
        createNodeRequest.set_path(path);
        Transaction_->AddTransactionAction(CellTagFromId(id), MakeTransactionActionData(createNodeRequest));
    }

    void AttachChild(TObjectId parentId, TObjectId childId, const TYPath& childKey)
    {
        NCypressServer::NProto::TReqAttachChild attachChildRequest;
        ToProto(attachChildRequest.mutable_parent_id(), parentId);
        ToProto(attachChildRequest.mutable_child_id(), childId);
        attachChildRequest.set_key(childKey);
        Transaction_->AddTransactionAction(CellTagFromId(parentId), MakeTransactionActionData(attachChildRequest));
    }

    TObjectId CreateSequential(
        TObjectId parentId,
        const TYPath& parentPath,
        const std::vector<TYPathBuf>& childKeys,
        std::function<TCellTag(TYPathBuf)> cellTagCallback)
    {
        auto childPath = parentPath;
        for (const auto& childKey : childKeys) {
            auto cellTag = cellTagCallback(childKey);
            childPath = YPathJoin(childPath, childKey);
            auto childId = Transaction_->GenerateObjectId(EObjectType::SequoiaMapNode, cellTag, /*sequoia*/ true);
            CreateNode(EObjectType::SequoiaMapNode, childId, childPath);
            AttachChild(parentId, childId, TYPath(childKey));
            parentId = childId;
        }
        return parentId;
    }

    void ValidateCreateOptions(
        const TCtxCreatePtr& context,
        const TReqCreate* request)
    {
        if (request->force()) {
            THROW_ERROR_EXCEPTION("Create with \"force\" flag is not supported in Sequoia yet");
        }
        if (request->ignore_existing()) {
            THROW_ERROR_EXCEPTION("Create with \"ignore_existing\" flag is not supported in Sequoia yet");
        }
        if (request->ignore_type_mismatch()) {
            THROW_ERROR_EXCEPTION("Create with \"ignore_type_mismatch\" flag is not supported in Sequoia yet");
        }
        if (request->lock_existing()) {
            THROW_ERROR_EXCEPTION("Create with \"lock_existing\" flag is not supported in Sequoia yet");
        }
        if (GetTransactionId(context->RequestHeader())) {
            THROW_ERROR_EXCEPTION("Create with transaction is not supported in Sequoia yet");
        }
    }
};

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, Create)
{
    ValidateCreateOptions(context, request);

    auto type = CheckedEnumCast<EObjectType>(request->type());

    if (type == EObjectType::SequoiaMapNode) {
        THROW_ERROR_EXCEPTION("%Qlv is internal type and should not be used directly; use %Qlv instead",
            EObjectType::SequoiaMapNode,
            EObjectType::MapNode);
    }

    if (type != EObjectType::MapNode && !IsScalarType(type)) {
        THROW_ERROR_EXCEPTION("Creation of %Qlv is not supported in Sequoia yet",
            type);
    }

    if (type == EObjectType::MapNode) {
        type = EObjectType::SequoiaMapNode;
    }

    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    auto pathTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);

    if (pathTokens.empty()) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::AlreadyExists,
            "%v already exists",
            Path_);
    }
    if (!request->recursive() && std::ssize(pathTokens) > 1) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Node %v has no child with key %Qv",
            Path_,
            pathTokens[0]);
    }

    const auto& parentPath = Path_;
    auto parentId = Id_;

    auto parentCellTag = CellTagFromId(parentId);

    // TODO(kvk1920): Choose cell tag properly.
    auto childCellTag = parentCellTag;

    // TODO(kvk1920): Support creating sequoia nodes on secondary cells.
    // Actually it is mostly done:
    // 1. Scion is not required to be at the same cell as rootstock.
    // 2. SequoiaMapNode too.
    // Other node types haven't been supported yet.
    auto primaryMasterCellTag = Bootstrap_->GetNativeConnection()->GetPrimaryMasterCellTag();
    if (parentCellTag != primaryMasterCellTag || childCellTag != primaryMasterCellTag) {
        THROW_ERROR_EXCEPTION("In Sequoia nodes can be created on primary cell only");
    }

    // Acquire shared lock on parent node.
    NRecords::TResolveNodeKey parentKey{
        .Path = MangleSequoiaPath(parentPath),
    };
    Transaction_->LockRow(parentKey, ELockType::SharedStrong);

    TYPathBuf childKey = pathTokens.back();
    pathTokens.pop_back();

    parentId = CreateSequential(
        parentId,
        parentPath,
        pathTokens,
        [&] (TYPathBuf /*childKey*/) {
            return childCellTag;
        });

    // NB: May differ from original path due to path rewrites.
    auto childPath = parentPath + unresolvedSuffix;

    // Generate new object id.
    auto childId = Transaction_->GenerateObjectId(type, childCellTag, /*sequoia*/ true);

    // Create child on destination cell.
    CreateNode(type, childId, childPath);
    // Attach child on parent cell.
    AttachChild(parentId, childId, TYPath(childKey));

    WaitFor(Transaction_->Commit({
            .CoordinatorCellId = GetObjectCellId(Id_),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
        .ThrowOnError();

    ToProto(response->mutable_node_id(), childId);
    response->set_cell_tag(ToProto<int>(childCellTag));
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, List)
{
    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    if (auto pathTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        !pathTokens.empty())
    {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Node %v has no child with key %Qv",
            Path_,
            pathTokens[0]);
    }

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    auto mangledPath = MangleSequoiaPath(Path_);
    Transaction_->LockRow(
        NRecords::TResolveNodeKey{.Path = mangledPath},
        ELockType::SharedStrong);

    auto selectRows = WaitFor(Transaction_->SelectRows<NRecords::TChildrenNodesKey>(
        {
            Format("parent_path = %Qv", mangledPath),
        },
        limit))
        .ValueOrThrow();

    WaitFor(Transaction_->Commit())
        .ThrowOnError();

    TStringStream out;
    NYson::TYsonWriter writer(&out);

    writer.OnBeginList();
    for (const auto& row : selectRows) {
        writer.OnListItem();
        writer.OnStringScalar(row.Key.ChildKey);
    }
    writer.OnEndList();

    response->set_value(out.Str());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    ISequoiaTransactionPtr transaction,
    TObjectId id,
    TYPath resolvedPath)
{
    auto proxy = New<TMapLikeNodeProxy>(bootstrap, id, std::move(resolvedPath), std::move(transaction));
    proxy->ValidateType();
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
