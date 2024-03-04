#include "node_proxy.h"

#include "action_helpers.h"
#include "bootstrap.h"
#include "helpers.h"
#include "path_resolver.h"
#include "private.h"
#include "sequoia_tree_visitor.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <util/random/random.h>

#include <stack>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

using TYPath = NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public TYPathServiceBase
    , public virtual TSupportsExists
    , public virtual TSupportsGet
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
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

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        // NB: In most cases resolve should be performed by Sequoia service.

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

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Set);
        DISPATCH_YPATH_SERVICE_METHOD(Remove);
        DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);

        return TYPathServiceBase::DoInvoke(context);
    }

    TCellId CellIdFromCellTag(TCellTag cellTag) const
    {
        return Bootstrap_->GetNativeConnection()->GetMasterCellId(cellTag);
    }

    TCellId CellIdFromObjectId(TObjectId id)
    {
        return Bootstrap_->GetNativeConnection()->GetMasterCellId(CellTagFromId(id));
    }

    TObjectServiceProxy CreateReadProxyToCell(TCellTag cellTag)
    {
        return CreateObjectServiceReadProxy(
            Bootstrap_->GetNativeRootClient(),
            EMasterChannelKind::Follower,
            cellTag,
            Bootstrap_->GetNativeConnection()->GetStickyGroupSizeCache());
    }

    TObjectServiceProxy CreateReadProxyForObject(TObjectId id)
    {
        return CreateObjectServiceReadProxy(
            Bootstrap_->GetNativeRootClient(),
            EMasterChannelKind::Follower,
            CellTagFromId(id),
            Bootstrap_->GetNativeConnection()->GetStickyGroupSizeCache());
    }

    TObjectServiceProxy CreateWriteProxyForObject(TObjectId id)
    {
        return CreateObjectServiceWriteProxy(
            Bootstrap_->GetNativeRootClient(),
            CellTagFromId(id));
    }

    void ValidateCreateOptions(
        const TCtxCreatePtr& context,
        const TReqCreate* request)
    {
        if (request->ignore_type_mismatch()) {
            THROW_ERROR_EXCEPTION("Create with \"ignore_type_mismatch\" flag is not supported in Sequoia yet");
        }
        if (request->lock_existing()) {
            THROW_ERROR_EXCEPTION("Create with \"lock_existing\" flag is not supported in Sequoia yet");
        }
        if (GetTransactionId(context->RequestHeader())) {
            THROW_ERROR_EXCEPTION("Create with transaction is not supported in Sequoia yet");
        }

        auto type = CheckedEnumCast<EObjectType>(request->type());
        if (type == EObjectType::SequoiaMapNode) {
            THROW_ERROR_EXCEPTION("%Qlv is internal type and should not be used directly; use %Qlv instead",
                EObjectType::SequoiaMapNode,
                EObjectType::MapNode);
        }
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

    template <class TRequestPtr, class TResponse, class TContextPtr>
    void ForwardRequest(TRequestPtr request, TResponse* response, const TContextPtr& context)
    {
        auto suffix = GetRequestTargetYPath(context->GetRequestHeader());
        SetRequestTargetYPath(&request->Header(), FromObjectId(Id_) + suffix);
        bool isMutating = IsRequestMutating(context->GetRequestHeader());
        auto proxy = isMutating ? CreateWriteProxyForObject(Id_) : CreateReadProxyForObject(Id_);

        YT_LOG_DEBUG("Forwarded request to master (RequestId: %v -> %v)",
            context->GetRequestId(),
            request->GetRequestId());

        auto rsp = WaitFor(proxy.Execute(std::move(request)))
            .ValueOrThrow();
        response->CopyFrom(*rsp);
        context->Reply();
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();
        auto limit = YT_PROTO_OPTIONAL(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        auto newRequest = TYPathProxy::Get();
        newRequest->CopyFrom(*request);
        ForwardRequest(std::move(newRequest), response, context);
    }

    void SetSelf(TReqSet* request, TRspSet* /*response*/, const TCtxSetPtr& context) override
    {
        bool force = request->force();
        context->SetRequestInfo("Force: %v", force);

        NRecords::TPathToNodeIdKey selfKey{
            .Path = MangleSequoiaPath(Path_),
        };
        Transaction_->LockRow(selfKey, ELockType::Exclusive);

        SetNode(Id_, NYson::TYsonString(request->value()), Transaction_);

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromObjectId(Id_),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
            .ThrowOnError();

        context->Reply();
    }

    void RemoveSelf(
        TReqRemove* request,
        TRspRemove* /*response*/,
        const TCtxRemovePtr& context) override
    {
        auto recursive = request->recursive();
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            recursive,
            force);

        TNodeId parentId;
        TCellTag subtreeRootCell;
        if (TypeFromId(Id_) == EObjectType::Scion) {
            subtreeRootCell = RemoveRootstock();
        } else {
            auto [parentPath, _] = DirNameAndBaseName(Path_);
            LockRowInPathToIdTable(parentPath, Transaction_);

            parentId = LookupNodeId(parentPath, Transaction_);
            subtreeRootCell = CellTagFromId(parentId);
        }

        auto mangledPath = MangleSequoiaPath(Path_);

        // NB: For non-recursive removal we have to check if directory is empty.
        // This can be done via requesting just 2 rows.
        auto selectedRowsLimit = recursive ? std::nullopt : std::optional(2);

        auto nodesToRemove = WaitFor(Transaction_->SelectRows<NRecords::TPathToNodeIdKey>(
            {
                .Where = {
                    Format("path >= %Qv", mangledPath),
                    Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath))
                },
                .OrderBy = {"path"},
                .Limit = selectedRowsLimit
            }))
            .ValueOrThrow();
        YT_VERIFY(nodesToRemove.size() >= 1);

        if (!recursive && nodesToRemove.size() > 1) {
            THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
        }

        RemoveSelectedSubtree(
            nodesToRemove,
            Transaction_,
            /*removeRoot*/ true,
            parentId);

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromCellTag(subtreeRootCell),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
            .ThrowOnError();

        context->Reply();
    }

    void ExistsAttribute(
        const TYPath& /*path*/,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override
    {
        context->SetRequestInfo();
        auto newRequest = TYPathProxy::Exists();
        newRequest->CopyFrom(*request);
        ForwardRequest(std::move(newRequest), response, context);
    }

    void GetAttribute(
        const TYPath& /*path*/,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override
    {
        context->SetRequestInfo();
        auto newRequest = TYPathProxy::Get();
        newRequest->CopyFrom(*request);
        ForwardRequest(std::move(newRequest), response, context);
    }

    void SetAttribute(
        const TYPath& /*path*/,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override
    {
        context->SetRequestInfo();
        auto newRequest = TYPathProxy::Set();
        newRequest->CopyFrom(*request);
        ForwardRequest(std::move(newRequest), response, context);
    }

    void RemoveAttribute(
        const TYPath& /*path*/,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override
    {
        context->SetRequestInfo();
        auto newRequest = TYPathProxy::Remove();
        newRequest->CopyFrom(*request);
        ForwardRequest(std::move(newRequest), response, context);
    }
};

DEFINE_YPATH_SERVICE_METHOD(TNodeProxyBase, GetBasicAttributes)
{
    context->SetRequestInfo();
    auto newRequest = TObjectYPathProxy::GetBasicAttributes();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxyBase, Create)
{
    auto type = CheckedEnumCast<EObjectType>(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto recursive = request->recursive();
    auto force = request->force();
    auto ignoreTypeMismatch = request->ignore_type_mismatch();
    auto hintId = FromProto<TNodeId>(request->hint_id());
    auto transactionId = GetTransactionId(context->RequestHeader());

    context->SetRequestInfo(
        "Type: %v, IgnoreExisting: %v, LockExisting: %v, Recursive: %v, "
        "Force: %v, IgnoreTypeMismatch: %v, HintId: %v, TransactionId: %v",
        type,
        ignoreExisting,
        lockExisting,
        recursive,
        force,
        ignoreTypeMismatch,
        hintId,
        transactionId);

    ValidateCreateOptions(context, request);

    // This alert can be safely removed, since hintId is not used in this function.
    YT_LOG_ALERT_IF(hintId, "Hint ID was recieved on cypress proxy (HintId: %v)", hintId);

    if (type == EObjectType::MapNode) {
        type = EObjectType::SequoiaMapNode;
    }

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION("Creation of %Qlv is not supported in Sequoia yet",
            type);
    }

    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
    if (unresolvedSuffixTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // Existing Scion instead of SequoiaMapNode is OK when ignore_existing is set.
        auto thisType = TypeFromId(Id_);
        auto compatibleTypes = type == EObjectType::SequoiaMapNode && thisType == EObjectType::Scion;
        if (thisType != type && !force && !ignoreTypeMismatch && !compatibleTypes) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "%v already exists and has type %Qlv while node of %Qlv type is about to be created",
                Path_,
                thisType,
                type);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        WaitFor(Transaction_->Commit())
            .ThrowOnError();

        ToProto(response->mutable_node_id(), Id_);
        response->set_cell_tag(ToProto<int>(CellTagFromId(Id_)));

        context->SetResponseInfo("ExistingNodeId: %v",
            Id_);
        context->Reply();
        return;
    }

    if (!recursive && std::ssize(unresolvedSuffixTokens) > 1) {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
    }

    auto parentPath = Path_;
    auto parentId = Id_;
    auto requestedChildPath = Path_ + unresolvedSuffix;

    TString targetName;
    if (unresolvedSuffixTokens.empty() && force) {
        auto [updatedParentPath, updatedTargetName] = DirNameAndBaseName(Path_);
        parentPath = std::move(updatedParentPath);
        targetName = std::move(updatedTargetName);
        // TODO(h0pless): Maybe add parentId to resolve result, then it can be passed here to avoid another lookup.
        parentId = LookupNodeId(parentPath, Transaction_);

        auto removeFuture = RemoveSubtree(Path_, Transaction_);
        WaitFor(removeFuture)
            .ThrowOnError();
    } else {
        if (!IsSequoiaCompositeNodeType(TypeFromId(Id_))) {
            THROW_ERROR_EXCEPTION("%v cannot have children",
                Path_);
        }

        targetName = TYPath(unresolvedSuffixTokens.back());
        unresolvedSuffixTokens.pop_back();
    }

    LockRowInPathToIdTable(parentPath, Transaction_);

    auto intermediateParentId = CreateIntermediateNodes(
        parentPath,
        parentId,
        unresolvedSuffixTokens,
        Transaction_);

    auto childCellTag = Transaction_->GetRandomSequoiaNodeHostCellTag();
    auto childId = Transaction_->GenerateObjectId(type, childCellTag);
    CreateNode(
        type,
        childId,
        requestedChildPath,
        Transaction_);
    AttachChild(intermediateParentId, childId, targetName, Transaction_);

    WaitFor(Transaction_->Commit({
        .CoordinatorCellId = CellIdFromObjectId(parentId),
        .Force2PC = true,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();

    ToProto(response->mutable_node_id(), childId);
    response->set_cell_tag(ToProto<int>(childCellTag));

    // TODO(h0pless): Add account info here, currently impossible to integrate properly due to the fact
    // that there is no such attribute stored in Sequoia dynamic tables.
    context->SetResponseInfo("NodeId: %v, CellTag: %v",
        Id_,
        CellTagFromId(Id_));
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxyBase, Copy)
{
    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    if (ypathExt.additional_paths_size() != 1) {
        THROW_ERROR_EXCEPTION("Invalid number of additional paths");
    }

    const auto& originalSourcePath = ypathExt.additional_paths(0);
    auto options = FromProto<TCopyOptions>(*request);

    // These are handled on cypress proxy and are not needed on master.
    auto force = request->force();
    auto ignoreExisting = request->ignore_existing();
    auto recursive = request->recursive();

    // This one is unimplemented yet.
    auto lockExisting = request->lock_existing();

    context->SetRequestInfo("TransactionId: %v, PreserveAccount: %v, PreserveCreationTime: %v, "
        "PreserveModificationTime: %v, PreserveExpirationTime: %v, PreserveExpirationTimeout: %v, "
        "PreserveOwner: %v, PreserveAcl: %v, Recursive: %v, IgnoreExisting: %v, LockExisting: %v, "
        "Force: %v, PessimisticQuotaCheck: %v, Mode: %v, OriginalSourcePath: %v",
        GetTransactionId(context->RequestHeader()),
        options.PreserveAccount,
        options.PreserveCreationTime,
        options.PreserveModificationTime,
        options.PreserveExpirationTime,
        options.PreserveExpirationTimeout,
        options.PreserveOwner,
        options.PreserveAcl,
        recursive,
        ignoreExisting,
        lockExisting,
        force,
        options.PessimisticQuotaCheck,
        options.Mode,
        originalSourcePath);

    // TODO(h0pless): Actually support this option when transactions are introduced.
    if (lockExisting) {
        THROW_ERROR_EXCEPTION("Copy with \"lock_existing\" flag is not supported in Sequoia yet");
    }

    // TODO(h0pless): Support acl preservation. It has to be done here and in master.
    if (options.PreserveAcl) {
        THROW_ERROR_EXCEPTION("Copy with \"preserve_acl\" flag is not supported in Sequoia yet");
    }

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (ignoreExisting && options.Mode == ENodeCloneMode::Move) {
        // This practically never happens. Maybe consider adding YT_VERIFY here.
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_existing\" for move operation");
    }

    auto sourcePathResolveResult = ResolvePath(Transaction_, originalSourcePath);
    const auto* payload = std::get_if<TSequoiaResolveResult>(&sourcePathResolveResult);
    if (!payload) {
        // TODO(h0pless): Throw CrossCellAdditionalPath error once {Begin,End}Copy are working.
        THROW_ERROR_EXCEPTION("%v is not a sequoia object, Cypress-to-Sequoia copy is not supported yet", originalSourcePath);
    }

    // TODO(h0pless): This might not be the best solution in a long run, but it'll work for now.
    if (TypeFromId(payload->ResolvedPrefixNodeId) == EObjectType::Scion) {
        THROW_ERROR_EXCEPTION("Scion cannot be cloned");
    }

    // NB: Rewriting in case there were symlinks in the original source path.
    const auto& sourceRootPath = payload->ResolvedPrefix;
    if (!payload->UnresolvedSuffix.empty()) {
        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(payload->UnresolvedSuffix);
        ThrowNoSuchChild(sourceRootPath, unresolvedSuffixTokens[0]);
    }

    // Validate there are no duplicate or missing destination nodes.
    auto unresolvedDestinationSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    auto destinationSuffixDirectoryTokens = TokenizeUnresolvedSuffix(unresolvedDestinationSuffix);
    if (destinationSuffixDirectoryTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        WaitFor(Transaction_->Commit())
            .ThrowOnError();

        ToProto(response->mutable_node_id(), Id_);

        context->SetResponseInfo("ExistingNodeId: %v",
            Id_);
        context->Reply();
        return;
    }

    if (!recursive && std::ssize(destinationSuffixDirectoryTokens) > 1) {
        ThrowNoSuchChild(Path_, destinationSuffixDirectoryTokens[0]);
    }

    auto nodesToCopy = SelectSubtree(sourceRootPath, Transaction_);
    auto destinationRootPath = Path_ + unresolvedDestinationSuffix;
    auto parentPath = Path_;
    auto parentId = Id_;
    TString targetName;

    auto overwriteDestinationSubtree = destinationSuffixDirectoryTokens.empty() && force;
    if (overwriteDestinationSubtree) {
        auto [updatedParentPath, updatedTargetName] = DirNameAndBaseName(Path_);
        parentPath = std::move(updatedParentPath);
        targetName = std::move(updatedTargetName);
        parentId = LookupNodeId(parentPath, Transaction_);
    } else {
        if (!IsSequoiaCompositeNodeType(TypeFromId(Id_))) {
            THROW_ERROR_EXCEPTION("%v cannot have children",
                Path_);
        }

        targetName = destinationSuffixDirectoryTokens.back();
        destinationSuffixDirectoryTokens.pop_back();
    }

    std::vector<NRecords::TPathToNodeId> nodesToRemove;
    if (overwriteDestinationSubtree) {
        nodesToRemove = SelectSubtree(destinationRootPath, Transaction_);
        RemoveSelectedSubtree(
            nodesToRemove,
            Transaction_,
            /*removeRoot*/ true,
            parentId);
    }

    // Select returns sorted entries and destination subtree cannot include source subtree.
    // Thus to check that subtrees don't overlap it's enough to check source root with
    // first and last elements of the destination subtree.
    if (options.Mode == ENodeCloneMode::Move && (nodesToRemove.empty() ||
        sourceRootPath < DemangleSequoiaPath(nodesToRemove.front().Key.Path) ||
        DemangleSequoiaPath(nodesToRemove.back().Key.Path) < sourceRootPath))
    {
        // TODO(h0pless): Maybe add parentId to resolve result, then it can be passed here to avoid another lookup.
        RemoveSelectedSubtree(nodesToCopy, Transaction_);
    }

    LockRowInPathToIdTable(parentPath, Transaction_);

    auto bottommostCreatedNodeId = CreateIntermediateNodes(
        parentPath,
        parentId,
        destinationSuffixDirectoryTokens,
        Transaction_);

    auto destinationId = CopySubtree(nodesToCopy, sourceRootPath, destinationRootPath, options, Transaction_);

    AttachChild(bottommostCreatedNodeId, destinationId, targetName, Transaction_);

    WaitFor(Transaction_->Commit({
        .CoordinatorCellId = CellIdFromObjectId(parentId),
        .Force2PC = true,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();

    ToProto(response->mutable_node_id(), destinationId);

    context->SetResponseInfo("NodeId: %v", destinationId);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TMapLikeNodeProxy
    : public TNodeProxyBase
{
public:
    using TNodeProxyBase::TNodeProxyBase;

private:
    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, List);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TNodeProxyBase::DoInvoke(context);
    }

    // TODO(h0pless): This class can be moved to helpers.
    // It only uses Owner_->Transaction_, it's safe to change owner's type from proxy to transaction.
    class TTreeBuilder
        : public NYson::TForwardingYsonConsumer
    {
    public:
        explicit TTreeBuilder(TMapLikeNodeProxy* owner)
            : Owner_(owner)
        {
            YT_VERIFY(Owner_);
        }

        void BeginTree(const TYPath& rootPath)
        {
            YT_VERIFY(NodeStack_.size() == 0);

            auto [parentPath, thisName] = DirNameAndBaseName(rootPath);
            ParentPath_ = std::move(parentPath);
            Key_ = std::move(thisName);
        }

        TNodeId EndTree()
        {
            // Failure here means that the tree is not fully constructed yet.
            YT_VERIFY(NodeStack_.size() == 0);
            return ResultNodeId_;
        }

        void OnMyStringScalar(TStringBuf value) override
        {
            auto nodeId = CreateNode(EObjectType::StringNode);
            SetValue(nodeId, NYson::ConvertToYsonString(value));
            AddNode(nodeId, false);
        }

        void OnMyInt64Scalar(i64 value) override
        {
            auto nodeId = CreateNode(EObjectType::Int64Node);
            SetValue(nodeId, NYson::ConvertToYsonString(value));
            AddNode(nodeId, false);
        }

        void OnMyUint64Scalar(ui64 value) override
        {
            auto nodeId = CreateNode(EObjectType::Uint64Node);
            SetValue(nodeId, NYson::ConvertToYsonString(value));
            AddNode(nodeId, false);
        }

        void OnMyDoubleScalar(double value) override
        {
            auto nodeId = CreateNode(EObjectType::DoubleNode);
            SetValue(nodeId, NYson::ConvertToYsonString(value));
            AddNode(nodeId, false);
        }

        void OnMyBooleanScalar(bool value) override
        {
            auto nodeId = CreateNode(EObjectType::BooleanNode);
            SetValue(nodeId, NYson::ConvertToYsonString(value));
            AddNode(nodeId, false);
        }

        void OnMyEntity() override
        {
            THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Sequoia");
        }

        void OnMyBeginList() override
        {
            THROW_ERROR_EXCEPTION("List nodes cannot be created inside Sequoia");
        }

        void OnMyBeginMap() override
        {
            auto nodeId = CreateNode(EObjectType::SequoiaMapNode);
            AddNode(nodeId, true);
            ParentPath_ = Format("%v/%v", ParentPath_, Key_);
        }

        void OnMyKeyedItem(TStringBuf key) override
        {
            Key_ = TString(key);
        }

        void OnMyEndMap() override
        {
            const auto& key = NodeStack_.top().first;
            ParentPath_ = ParentPath_.substr(0, ParentPath_.size() - key.size() - 1);
            NodeStack_.pop();
        }

        void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("Set with attributes is not supported in Sequoia yet");
        }

    private:
        const TMapLikeNodeProxy* Owner_;
        TString Key_;
        TYPath ParentPath_;
        TNodeId ResultNodeId_;
        std::stack<std::pair<TString, TNodeId>> NodeStack_;

        TNodeId CreateNode(EObjectType type)
        {
            auto nodeId = Owner_->Transaction_->GenerateObjectId(type);
            NCypressProxy::CreateNode(type, nodeId, Format("%v/%v", ParentPath_, Key_), Owner_->Transaction_);
            return nodeId;
        }

        void SetValue(TNodeId nodeId, const NYson::TYsonString& value)
        {
            SetNode(nodeId, value, Owner_->Transaction_);
        }

        void AddNode(TNodeId nodeId, bool push)
        {
            if (NodeStack_.empty()) {
                ResultNodeId_ = nodeId;
            } else {
                auto parentId = NodeStack_.top().second;
                AttachChild(parentId, nodeId, Key_, Owner_->Transaction_);
            }

            if (push) {
                NodeStack_.emplace(Key_, nodeId);
            }
        }
    };

    class TMapNodeSetter
        : public TTypedConsumer
    {
    public:
        TMapNodeSetter(TMapLikeNodeProxy* owner)
            : Owner_(owner)
            , TreeBuilder_(owner)
        {
            YT_VERIFY(Owner_);
        }

        void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("Set with attributes is not supported in Sequoia yet");
        }

    private:
        TMapLikeNodeProxy* Owner_;
        TTreeBuilder TreeBuilder_;
        TString ChildKey_;
        THashMap<TString, TNodeId> Children_;
        TFuture<void> ClearRequest_;

        ENodeType GetExpectedType() override
        {
            return ENodeType::Map;
        }

        void OnMyBeginMap() override
        {
            ClearRequest_ = RemoveSubtree(
                Owner_->Path_,
                Owner_->Transaction_,
                /*removeRoot*/ false,
                Owner_->Id_);
        }

        void OnMyKeyedItem(TStringBuf key) override
        {
            THROW_ERROR_EXCEPTION_IF(
                Children_.contains(key),
                "Node %Qv already exists",
                key);

            auto subtreeRootPath = Format("%v/%v", Owner_->Path_, key);
            TreeBuilder_.BeginTree(subtreeRootPath);
            Forward(&TreeBuilder_, std::bind(&TMapNodeSetter::OnForwardingFinished, this, TString(key)));
        }

        void OnForwardingFinished(TString itemKey)
        {
            auto childId = TreeBuilder_.EndTree();
            EmplaceOrCrash(Children_, std::move(itemKey), childId);
        }

        void OnMyEndMap() override
        {
            for (const auto& [childKey, childId] : Children_) {
                AttachChild(Owner_->Id_, childId, childKey, Owner_->Transaction_);
            }
            WaitFor(ClearRequest_)
                .ThrowOnError();
        }
    };

    void SetSelf(TReqSet* request, TRspSet* /*response*/, const TCtxSetPtr& context) override
    {
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            request->recursive(),
            force);

        if (!force) {
            THROW_ERROR_EXCEPTION("\"set\" command without \"force\" flag is forbidden; use \"create\" instead");
        }

        NRecords::TPathToNodeIdKey selfKey{
            .Path = MangleSequoiaPath(Path_),
        };
        Transaction_->LockRow(selfKey, ELockType::Exclusive);

        TMapNodeSetter setter(this);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&setter);

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromObjectId(Id_),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
            .ThrowOnError();

        context->Reply();
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto limit = YT_PROTO_OPTIONAL(*request, limit);
        // NB: This is an arbitrary value, it can be freely changed.
        // TODO(h0pless): Think about moving global limit to dynamic config.
        i64 responseSizeLimit = limit ? *limit : 100'000;

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        // Fetch nodes from child nodes table.
        std::queue<TNodeId> childrenLookupQueue;
        childrenLookupQueue.push(Id_);

        THashMap<TNodeId, std::vector<NRecords::TChildNode>> nodeIdToChildren;
        nodeIdToChildren[Id_] = {};

        int maxRetrievedDepth = 0;

        // NB: 1 node is root node and it should not count towards the limit.
        // If the number of nodes in a subtree of certain depth it equal to the limit, then we should
        // fetch the next layer, so opaques can be set correctly.
        while (std::ssize(nodeIdToChildren) <= responseSizeLimit + 1) {
            std::vector<TFuture<std::vector<NRecords::TChildNode>>> asyncNextLayer;
            while (!childrenLookupQueue.empty()) {
                auto childrenFuture = Transaction_->SelectRows<NRecords::TChildNodeKey>(
                    {
                        .Where = {Format("parent_id = %Qv", childrenLookupQueue.front())},
                        .OrderBy = {"parent_id", "child_key"}
                    });
                childrenLookupQueue.pop();
                asyncNextLayer.push_back(std::move(childrenFuture));
            }

            // This means that we finished tree traversal.
            if (asyncNextLayer.empty()) {
                break;
            }

            // This should lead to a retry, but retries are not implemented in Sequoia yet.
            // TODO(h0pless): Update error once Sequoia retries are implemented.
            auto currentSubtreeLayerChildren = WaitFor(AllSucceeded(asyncNextLayer))
                .ValueOrThrow();

            for (const auto& children : currentSubtreeLayerChildren) {
                for (const auto& child : children) {
                    nodeIdToChildren[child.Key.ParentId].push_back(child);
                    nodeIdToChildren[child.ChildId] = {};
                    childrenLookupQueue.push(child.ChildId);
                }
            }

            ++maxRetrievedDepth;
        }

        // Find all nodes that need to be requested from master cells.
        THashMap<TCellTag, std::vector<TNodeId>> cellTagToNodeIds;
        for (const auto& [nodeId, _] : nodeIdToChildren) {
            auto nodeType = TypeFromId(nodeId);
            if (IsScalarType(nodeType) || attributeFilter) {
                cellTagToNodeIds[CellTagFromId(nodeId)].push_back(nodeId);
            }
        }

        // Send master requests.
        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        asyncResults.reserve(cellTagToNodeIds.size());
        for (const auto& [cellTag, nodeIds] : cellTagToNodeIds) {
            auto proxy = CreateReadProxyToCell(cellTag);
            // TODO(h0pless): Create a new type of request to make Get request amplification less drastic (see YT-20911).
            auto batchReq = proxy.ExecuteBatch();

            for (auto nodeId : nodeIds) {
                auto masterRequest = TYPathProxy::Get(FromObjectId(nodeId));
                // NB: When copying only the body of a given request gets copied, header stays intact.
                // And path or id are stored in the header.
                masterRequest->CopyFrom(*request);
                masterRequest->Tag() = nodeId;
                batchReq->AddRequest(masterRequest);
            }
            asyncResults.push_back(batchReq->Invoke());
        }

        auto results = WaitFor(AllSucceeded(asyncResults))
            .ValueOrThrow();

        THashMap<TNodeId, TYPathProxy::TRspGetPtr> nodeIdToMasterResponse;
        nodeIdToMasterResponse.reserve(cellTagToNodeIds.size());
        for (const auto& batchRsp : results) {
            // TODO(kvk1920): In case of race between Get(path) and Create(path, force=true)
            // for the same path we can get an error "no such node".
            // Retry is needed if a given path still exists.
            // Since retry mechanism is not implemented yet, this will do for now.
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRsp), "Error getting requested information from master");

            for (const auto& rspOrError : batchRsp->GetResponses<TYPathProxy::TRspGet>()) {
                const auto& rsp = rspOrError.Value();
                auto nodeId = std::any_cast<TNodeId>(rsp->Tag());
                nodeIdToMasterResponse[nodeId] = rsp;
            }
        }

        // Build a DFS over this mess.
        TStringStream stream;
        TYsonWriter writer(&stream);

        VisitSequoiaTree(
            Id_,
            maxRetrievedDepth,
            &writer,
            attributeFilter,
            std::move(nodeIdToChildren),
            std::move(nodeIdToMasterResponse));

        writer.Flush();

        response->set_value(stream.Str());
        context->Reply();
    }

    void GetRecursive(
        const TYPath& path,
        TReqGet* request,
        TRspGet* /*response*/,
        const TCtxGetPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto limit = YT_PROTO_OPTIONAL(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        // There is no composite node type other than Sequoia map node. If we
        // have unresolved suffix it can be either attribute or non-existent child.
        ThrowNoSuchChild(Path_, tokenizer.GetLiteralValue());
    }

    void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* /*response*/,
        const TCtxSetPtr& context) override
    {
        // TODO(danilalexeev): Implement method _SetChild_ and bring out the common code with Create.
        auto recursive = request->recursive();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            recursive,
            request->force());

        auto unresolvedSuffix = "/" + path;
        auto destinationPath = Path_ + unresolvedSuffix;
        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        auto targetName = unresolvedSuffixTokens.back();
        unresolvedSuffixTokens.pop_back();

        if (!recursive && !unresolvedSuffixTokens.empty()) {
            ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
        }

        // Acquire shared lock on parent node.
        LockRowInPathToIdTable(Path_, Transaction_);

        auto targetParentId = CreateIntermediateNodes(
            Path_,
            Id_,
            unresolvedSuffixTokens,
            Transaction_);

        TTreeBuilder builder(this);
        builder.BeginTree(destinationPath);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&builder);
        auto targetNodeId = builder.EndTree();

        AttachChild(
            targetParentId,
            targetNodeId,
            TYPath(targetName),
            Transaction_);

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromObjectId(Id_),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
            .ThrowOnError();

        context->Reply();
    }

    void RemoveRecursive(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* /*response*/,
        const TCtxRemovePtr& context) override
    {
        auto recursive = request->recursive();
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            recursive,
            force);

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        // There is no composite node type other than Sequoia map node. If we
        // have unresolved suffix it can be either attribute or non-existent child.
        // Flag force was specifically designed to ignore this error.
        if (!force) {
            ThrowNoSuchChild(Path_, tokenizer.GetLiteralValue());
        }
        context->Reply();
    }
};

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, List)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    auto limit = YT_PROTO_OPTIONAL(*request, limit);

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
        limit,
        attributeFilter);

    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    if (auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        !unresolvedSuffixTokens.empty())
    {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
    }

    LockRowInPathToIdTable(Path_, Transaction_);
    auto selectRows = WaitFor(Transaction_->SelectRows<NRecords::TChildNodeKey>({
        .Where = {Format("parent_id = %Qv", Id_)},
        .OrderBy = {"parent_id", "child_key"},
        .Limit = limit
    }))
        .ValueOrThrow();

    // NB: Transaction with no participants has a fast-path for commit, making it equivalent to abort.
    WaitFor(Transaction_->Commit())
        .ThrowOnError();

    response->set_value(BuildYsonStringFluently()
        .BeginList()
            .DoFor(selectRows, [&] (TFluentList fluent, const auto& row) {
                fluent
                    .Item().Value(row.Key.ChildKey);
            })
        .EndList().ToString());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeSequoiaProxy
    : public TNodeProxyBase
{
public:
    using TNodeProxyBase::TNodeProxyBase;

private:
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, BeginUpload);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, EndUpload);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, GetUploadParams);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(BeginUpload);
        DISPATCH_YPATH_SERVICE_METHOD(EndUpload);
        DISPATCH_YPATH_SERVICE_METHOD(GetUploadParams);

        return TNodeProxyBase::DoInvoke(context);
    }
};

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeSequoiaProxy, BeginUpload)
{
    context->SetRequestInfo();
    auto newRequest = TChunkOwnerYPathProxy::BeginUpload();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeSequoiaProxy, EndUpload)
{
    context->SetRequestInfo();
    auto newRequest = TChunkOwnerYPathProxy::EndUpload();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeSequoiaProxy, GetUploadParams)
{
    context->SetRequestInfo();
    auto newRequest = TChunkOwnerYPathProxy::GetUploadParams();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    ISequoiaTransactionPtr transaction,
    TObjectId id,
    TYPath resolvedPath)
{
    auto type = TypeFromId(id);
    ValidateSupportedSequoiaType(type);
    // TODO(danilalexeev): Think of a better way of dispatch.
    TIntrusivePtr<TNodeProxyBase> proxy;
    if (IsSequoiaCompositeNodeType(type)) {
        proxy = New<TMapLikeNodeProxy>(bootstrap, id, std::move(resolvedPath), std::move(transaction));
    } else if (IsChunkOwnerType(type)) {
        proxy = New<TChunkOwnerNodeSequoiaProxy>(bootstrap, id, std::move(resolvedPath), std::move(transaction));
    } else {
        proxy = New<TNodeProxyBase>(bootstrap, id, std::move(resolvedPath), std::move(transaction));
    }
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
