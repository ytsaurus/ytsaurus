#include "node_proxy.h"

#include "private.h"

#include "bootstrap.h"
#include "helpers.h"
#include "node_proxy_base.h"
#include "path_resolver.h"
#include "sequoia_tree_visitor.h"
#include "sequoia_session.h"
#include "sequoia_service.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/exception_helpers.h>
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

using TYPath = NSequoiaClient::TYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_YPATH_CONTEXT_IMPL(ISequoiaServiceContext, TTypedSequoiaServiceContext);

DECLARE_SUPPORTS_METHOD(Get);
DECLARE_SUPPORTS_METHOD(Set);
DECLARE_SUPPORTS_METHOD(Remove);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

IMPLEMENT_SUPPORTS_METHOD(Get)
IMPLEMENT_SUPPORTS_METHOD(Set)
IMPLEMENT_SUPPORTS_METHOD(Remove)

IMPLEMENT_SUPPORTS_METHOD_RESOLVE(
    Exists,
    {
        context->SetRequestInfo();
        // An empty stream indicates that the object exists. Paths that end up
        // being resolved here and must return a positive resoponse are usually
        // those with a trailing '&'. Note that "&" is already skipped somewhere
        // inside these macros.
        Reply(context, /*exists*/ tokenizer.GetType() == NYPath::ETokenType::EndOfStream);
    })


void TSupportsExists::ExistsAttribute(
    const NYPath::TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ false);
}

void TSupportsExists::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ true);
}

void TSupportsExists::ExistsRecursive(
    const NYPath::TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, /*exists*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TNodeProxy
    : public TNodeProxyBase
    , public TSupportsExists
    , public TSupportsGet
    , public TSupportsSet
    , public TSupportsRemove
{
public:
    TNodeProxy(
        IBootstrap* bootstrap,
        TSequoiaSessionPtr session,
        TSequoiaResolveResult resolveResult)
        : TNodeProxyBase(bootstrap, std::move(session))
        , Bootstrap_(bootstrap)
        , Id_(resolveResult.Id)
        , Path_(resolveResult.Path)
        , ParentId_(resolveResult.ParentId)
        , ResolveResult_(std::move(resolveResult))
    {
        // TODO(kvk1920): support snapshot branches here.
        auto nodeType = TypeFromId(Id_);
        YT_VERIFY(ParentId_ || nodeType == EObjectType::Scion || nodeType == EObjectType::Link);
    }

protected:
    IBootstrap* const Bootstrap_;
    const TNodeId Id_;
    const TAbsoluteYPath Path_;
    // Can be null only if |Id_| is a scion, Cypress symlink or snapshot branch.
    const TNodeId ParentId_;
    const TSequoiaResolveResult ResolveResult_;

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Set);
        DISPATCH_YPATH_SERVICE_METHOD(Remove);
        DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);

        return false;
    }

    TCellId CellIdFromCellTag(TCellTag cellTag) const
    {
        return Bootstrap_->GetNativeConnection()->GetMasterCellId(cellTag);
    }

    TCellId CellIdFromObjectId(TObjectId id)
    {
        return CellIdFromCellTag(CellTagFromId(id));
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
        return CreateReadProxyToCell(CellTagFromId(id));
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

        // TODO(kvk1920): think about transactions.

        // Scion removal causes rootstock removal.
        // Since rootstock's parent _always_ lives at the same cell as rootstock
        // `DetachChild()` isn't needed.

        // TODO(kvk1920): Think about inferring rootstock's id from scion's one.
        // TODO(kvk1920): make it a part of |TSequoiaSession::RemoveRootstock|.
        auto reqGet = TYPathProxy::Get(FromObjectId(Id_) + "/@rootstock_id");
        auto rspGet = WaitFor(CreateReadProxyForObject(Id_).Execute(reqGet))
            .ValueOrThrow();
        auto rootstockId = ConvertTo<TNodeId>(NYson::TYsonString(rspGet->value()));

        return SequoiaSession_->RemoveRootstock(rootstockId);
    }

    template <class TRequestPtr, class TResponse, class TContextPtr>
    void ForwardRequest(TRequestPtr request, TResponse* response, const TContextPtr& context)
    {
        // TODO(kvk1920): it could be better to deal with Cypress tx replication
        // here since we already have started Sequoia tx. This will require a
        // request flag "suppress_mirrored_tx_sync" or something similar.

        auto suffix = GetRequestTargetYPath(context->GetRequestHeader());
        SetRequestTargetYPath(&request->Header(), FromObjectId(Id_) + suffix);
        bool isMutating = IsRequestMutating(context->GetRequestHeader());
        auto proxy = isMutating ? CreateWriteProxyForObject(Id_) : CreateReadProxyForObject(Id_);

        // TODO(kvk1920): it always prints "0-0-0-0 -> 0-0-0-0". Investigate it.

        YT_LOG_DEBUG("Forwarded request to master (RequestId: %v -> %v)",
            context->GetRequestId(),
            request->GetRequestId());

        auto rsp = WaitFor(proxy.Execute(std::move(request)))
            .ValueOrThrow();
        response->CopyFrom(*rsp);
        context->Reply();
    }


    struct TSubtreeReplacementResult
    {
        //! The target node (or subtree) will be a child of this node.
        TNodeId TargetParentId;
        //! This node is used to determine Sequoia tx coordinator.
        /*!
         *  NB: it's not the same as |TargetParentId|. Let's consider recursive
         *  creation "create map_node //a/b/c --recursive" where "//a" is
         *  already exists. In this case //a is attachment point and //a/b is
         *  target's parent.
         */
        TNodeId AttachmentPointNodeId;
        //! The name of target node (or subtree root) to create.
        TString TargetNodeKey;
    };

    //! Replaces subtree with (maybe empty) chain of map-nodes and locks
    //! attachment point's row in "node_id_to_path" table.
    //! Optional out parameter #removedNodes is used to report removed subtree.
    /*!
     *  On node creation or subtree copying we have to do the similar actions.
     *  For example, let's look at node creation:
     *  "create //a/b/c --force --recursive".
     *
     *  There are 2 major cases:
     *    1. some ancestor of target node exists but parent does not;
     *    2. target node exists (may be with its own subtree).
     *
     *  In the first case we need to create a chain of map nodes:
     *    //a/b - exists;
     *    //a/b/c/d/e - creating;
     *    /c/d - map-node chain which is created in this function.
     *    /a/b should be locked.
     *
     *  In the second case we need to remove current node and its subtree.
     *    Existed: //a/b, //a/b/c, //a/b/d
     *    Being created: //a/b
     *    //a/b, //a/b/c and //a/b/d have to be removed.
     *    //a should be locked.
     */
    TSubtreeReplacementResult ReplaceSubtreeWithMapNodeChain(
        TRange<TString> unresolvedSuffixTokens,
        bool force,
        std::vector<NRecords::TPathToNodeId>* removedNodes = nullptr)
    {
        // Inplace.
        if (unresolvedSuffixTokens.Empty()) {
            YT_VERIFY(force);

            auto subtreeToRemove = SequoiaSession_->SelectSubtree(Path_);
            SequoiaSession_->DetachAndRemoveSubtree(subtreeToRemove, ParentId_);

            if (removedNodes) {
                *removedNodes = std::move(subtreeToRemove.Records);
            }

            return {
                .TargetParentId = ParentId_,
                .AttachmentPointNodeId = ParentId_,
                .TargetNodeKey = Path_.GetBaseName(),
            };
        }

        if (!IsSequoiaCompositeNodeType(TypeFromId(Id_))) {
            THROW_ERROR_EXCEPTION("%v cannot have children",
                Path_);
        }

        return {
            // NB: |TargetParentId| is locked in CreateMapNodeChain().
            .TargetParentId = SequoiaSession_->CreateMapNodeChain(
                Path_,
                Id_,
                unresolvedSuffixTokens.Slice(0, unresolvedSuffixTokens.Size() - 1)),
            .AttachmentPointNodeId = Id_,
            .TargetNodeKey = unresolvedSuffixTokens.Back(),
        };
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

        SequoiaSession_->SetNode(Id_, NYson::TYsonString(request->value()));

        SequoiaSession_->Commit(CellIdFromObjectId(Id_));

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

        TCellTag subtreeRootCell;
        if (TypeFromId(Id_) == EObjectType::Scion) {
            subtreeRootCell = RemoveRootstock();
        } else {
            YT_VERIFY(ParentId_);
            subtreeRootCell = CellTagFromId(ParentId_);
        }

        if (recursive) {
            auto subtree = SequoiaSession_->SelectSubtree(Path_);
            YT_VERIFY(!subtree.Records.empty());
            SequoiaSession_->DetachAndRemoveSubtree(subtree, ParentId_);
        } else if (!SequoiaSession_->IsMapNodeEmpty(Id_)) {
            THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
        } else {
            SequoiaSession_->DetachAndRemoveSingleNode(Id_, Path_, ParentId_);
        }

        // Detaching child for subtree root should be done in late prepare.
        SequoiaSession_->Commit(CellIdFromCellTag(subtreeRootCell));

        context->Reply();
    }

    void ExistsAttribute(
        const NYPath::TYPath& /*path*/,
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
        const NYPath::TYPath& /*path*/,
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
        const NYPath::TYPath& /*path*/,
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
        const NYPath::TYPath& /*path*/,
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

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, GetBasicAttributes)
{
    context->SetRequestInfo();
    auto newRequest = TObjectYPathProxy::GetBasicAttributes();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Create)
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

    // This alert can be safely removed since hintId is not used in this function.
    YT_LOG_ALERT_IF(hintId, "Hint ID was received on Cypress proxy (HintId: %v)", hintId);

    if (type == EObjectType::MapNode) {
        type = EObjectType::SequoiaMapNode;
    }

    auto explicitAttributes = request->has_node_attributes()
        ? NYTree::FromProto(request->node_attributes())
        : CreateEphemeralAttributes();

    if (type == EObjectType::Link) {
        ValidateLinkNodeCreation(
            SequoiaSession_,
            explicitAttributes->Get<TRawYPath>(EInternedAttributeKey::TargetPath.Unintern()),
            ResolveResult_);
        type = EObjectType::SequoiaLink;
    }

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION("Creation of %Qlv is not supported in Sequoia yet",
            type);
    }

    auto unresolvedSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
    if (unresolvedSuffixTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // Node already exists but we still have to check its type.

        // Existing Scion instead of SequoiaMapNode is OK when ignore_existing is set.
        auto thisType = TypeFromId(Id_);
        auto compatibleTypes =
            type == EObjectType::SequoiaMapNode && thisType == EObjectType::Scion;
        if (!ignoreTypeMismatch && thisType != type && !compatibleTypes) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "%v already exists and has type %Qlv while node of %Qlv type is about to be created",
                Path_,
                thisType,
                type);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        SequoiaSession_->Commit();

        ToProto(response->mutable_node_id(), Id_);
        response->set_cell_tag(ToProto<int>(CellTagFromId(Id_)));

        context->SetResponseInfo("ExistingNodeId: %v", Id_);
        context->Reply();
        return;
    }

    if (!recursive && unresolvedSuffixTokens.size() > 1) {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
    }

    auto [targetParentNodeId, attachmentPointNodeId, targetKey] = ReplaceSubtreeWithMapNodeChain(
        unresolvedSuffixTokens,
        force);

    auto createdNodeId = SequoiaSession_->CreateNode(
        type,
        Path_ + unresolvedSuffix,
        explicitAttributes.Get(),
        targetParentNodeId);

    SequoiaSession_->Commit(CellIdFromObjectId(attachmentPointNodeId));

    ToProto(response->mutable_node_id(), createdNodeId);
    response->set_cell_tag(ToProto<int>(CellTagFromId(createdNodeId)));

    // TODO(h0pless): Add account info here, currently impossible to integrate properly due to the fact
    // that there is no such attribute stored in Sequoia dynamic tables.
    context->SetResponseInfo("NodeId: %v, CellTag: %v",
        createdNodeId,
        CellTagFromId(createdNodeId));
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Copy)
{
    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    if (ypathExt.additional_paths_size() != 1) {
        THROW_ERROR_EXCEPTION("Invalid number of additional paths");
    }

    auto originalSourcePath = TAbsoluteYPathBuf(ypathExt.additional_paths(0));
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

    auto sourceResolveResult = ResolvePath(SequoiaSession_, originalSourcePath.ToRawYPath(), "Copy");

    const auto* resolvedSource = std::get_if<TSequoiaResolveResult>(&sourceResolveResult);
    if (!resolvedSource) {
        // Initiate cross-cell copy here.
        // TODO(h0pless): Throw CrossCellAdditionalPath error once {Begin,End}Copy are working.
        THROW_ERROR_EXCEPTION("%v is not a sequoia object, Cypress-to-Sequoia copy is not supported yet",
            originalSourcePath);
    }

    // TODO(h0pless): This might not be the best solution in a long run, but it'll work for now.
    // Clarification: we need to convert scion into sequioa map node, currently we can't do that.
    if (TypeFromId(resolvedSource->Id) == EObjectType::Scion) {
        THROW_ERROR_EXCEPTION("Scion cannot be cloned");
    }

    // NB: Rewriting in case there were symlinks in the original source path.
    const auto& sourceRootPath = resolvedSource->Path;
    if (auto sourceUnresolvedSuffix = resolvedSource->UnresolvedSuffix;
        !sourceUnresolvedSuffix.IsEmpty())
    {
        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(sourceUnresolvedSuffix);
        ThrowNoSuchChild(sourceRootPath, unresolvedSuffixTokens[0]);
    }

    // NB: from now all symlinks are resolved and all paths don't contain
    // symlinks so we can just compare paths here.
    if (Path_ == sourceRootPath) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to itself");
    }
    if (Path_.Underlying().StartsWith(sourceRootPath.Underlying())) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to its descendant");
    }

    // Validate there are no duplicate or missing destination nodes.
    auto unresolvedDestinationSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    auto destinationSuffixDirectoryTokens = TokenizeUnresolvedSuffix(unresolvedDestinationSuffix);
    if (destinationSuffixDirectoryTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        SequoiaSession_->Commit();

        ToProto(response->mutable_node_id(), Id_);

        context->SetResponseInfo("ExistingNodeId: %v", Id_);
        context->Reply();
        return;
    }

    if (!recursive && std::ssize(destinationSuffixDirectoryTokens) > 1) {
        ThrowNoSuchChild(Path_, destinationSuffixDirectoryTokens[0]);
    }

    std::vector<NRecords::TPathToNodeId> removedNodes;
    auto [destinationParentId, attachmentPointNodeId, targetKey] = ReplaceSubtreeWithMapNodeChain(
        destinationSuffixDirectoryTokens,
        force,
        &removedNodes);

    auto nodesToCopy = SequoiaSession_->SelectSubtree(sourceRootPath);

    // Select returns sorted entries and destination subtree cannot include source subtree.
    // Thus to check that subtrees don't overlap it's enough to check source root with
    // first and last elements of the destination subtree.
    if (options.Mode == ENodeCloneMode::Move &&
        (removedNodes.empty() ||
         sourceRootPath < TAbsoluteYPath(removedNodes.front().Key.Path) ||
         TAbsoluteYPath(removedNodes.back().Key.Path) < sourceRootPath))
    {
        auto sourceParentId = resolvedSource->ParentId;
        // Since source is not a scion (because they cannot be copied) it has at
        // least one ancestor in Sequoia.
        YT_VERIFY(sourceParentId);

        SequoiaSession_->DetachAndRemoveSubtree(nodesToCopy, sourceParentId);
    }

    auto destinationRootPath = Path_ + unresolvedDestinationSuffix;
    auto destinationId = SequoiaSession_->CopySubtree(
        nodesToCopy,
        sourceRootPath,
        destinationRootPath,
        destinationParentId,
        options);

    SequoiaSession_->Commit(CellIdFromObjectId(attachmentPointNodeId));

    ToProto(response->mutable_node_id(), destinationId);

    context->SetResponseInfo("NodeId: %v", destinationId);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TMapLikeNodeProxy
    : public TNodeProxy
{
public:
    using TNodeProxy::TNodeProxy;

private:
    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, List);

    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TNodeProxy::DoInvoke(context);
    }

    // TODO(h0pless): This class can be moved to helpers.
    // It only uses Owner_->SequoiaSession_, it's safe to change owner's type from proxy to transaction.
    //! This class consumes YSON, builds tree and attaches it to its parent.
    class TTreeBuilder
        : public NYson::TForwardingYsonConsumer
    {
    public:
        // NB: if #subtreePath is "//a/b/c" then #parentId is ID of "//a/b".
        explicit TTreeBuilder(
            TSequoiaSession* session,
            TAbsoluteYPath subtreePath,
            TNodeId parentId)
            : Session_(session)
            , CurrentPath_(std::move(subtreePath))
        {
            YT_VERIFY(Session_);

            CurrentAncestors_.push(parentId);
        }

        ~TTreeBuilder() noexcept
        {
            // Failure here means that the tree is not fully constructed yet.
            YT_VERIFY(CurrentAncestors_.size() == 1);
        }

        void OnMyKeyedItem(TStringBuf key) override
        {
            CurrentPath_.Append(ToStringLiteral(key));
        }

        void OnMyBeginMap() override
        {
            auto nodeId = CreateNode(EObjectType::SequoiaMapNode);
            CurrentAncestors_.push(nodeId);
        }

        void OnMyEndMap() override
        {
            CurrentPath_ = CurrentPath_.GetDirPath();
            CurrentAncestors_.pop();
        }

        void OnMyStringScalar(TStringBuf value) override
        {
            CreateNonCompositeNodeAndPopItsKey(EObjectType::StringNode, value);
        }

        void OnMyInt64Scalar(i64 value) override
        {
            CreateNonCompositeNodeAndPopItsKey(EObjectType::Int64Node, value);
        }

        void OnMyUint64Scalar(ui64 value) override
        {
            CreateNonCompositeNodeAndPopItsKey(EObjectType::Uint64Node, value);
        }

        void OnMyDoubleScalar(double value) override
        {
            CreateNonCompositeNodeAndPopItsKey(EObjectType::DoubleNode, value);
        }

        void OnMyBooleanScalar(bool value) override
        {
            CreateNonCompositeNodeAndPopItsKey(EObjectType::BooleanNode, value);
        }

        void OnMyEntity() override
        {
            THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Sequoia");
        }

        void OnMyBeginList() override
        {
            THROW_ERROR_EXCEPTION("List nodes cannot be created inside Sequoia");
        }

        void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("Set with attributes is not supported in Sequoia yet");
        }

    private:
        TSequoiaSession* const Session_;
        std::stack<TNodeId, std::vector<TNodeId>> CurrentAncestors_;
        TAbsoluteYPath CurrentPath_;

        template <class T>
        void CreateNonCompositeNodeAndPopItsKey(EObjectType type, const T& value)
        {
            auto nodeId = CreateNode(type);
            Session_->SetNode(nodeId, NYson::ConvertToYsonString(value));

            CurrentPath_ = CurrentPath_.GetDirPath();
        }

        TNodeId CreateNode(EObjectType type)
        {
            return Session_->CreateNode(
                type,
                CurrentPath_,
                /*explicitAttributes*/ nullptr,
                CurrentAncestors_.top());
        }
    };

    class TMapNodeSetter
        : public TTypedConsumer
    {
    public:
        TMapNodeSetter(
            TSequoiaSession* session,
            TAbsoluteYPath path,
            TNodeId nodeId)
            : Session_(session)
            , Path_(std::move(path))
            , Id_(nodeId)
        {
            YT_VERIFY(Session_);
            YT_VERIFY(TypeFromId(nodeId) == EObjectType::SequoiaMapNode);
        }

        void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("Set with attributes is not supported in Sequoia yet");
        }

    private:
        TSequoiaSession* const Session_;
        const TAbsoluteYPath Path_;
        const TNodeId Id_;
        std::optional<TTreeBuilder> SubtreeBuilderHolder_;

        void OnMyKeyedItem(TStringBuf key) override
        {
            YT_ASSERT(!SubtreeBuilderHolder_.has_value());

            auto subtreeRootPath = YPathJoin(Path_, ToStringLiteral(key));
            auto& builder = SubtreeBuilderHolder_.emplace(Session_, subtreeRootPath, Id_);
            Forward(&builder, [this] {
                SubtreeBuilderHolder_.reset();
            });
        }

        // TTypedConsumer implementation.
        ENodeType GetExpectedType() override
        {
            return ENodeType::Map;
        }

        void OnMyBeginMap() override
        { }

        void OnMyEndMap() override
        { }
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

        // NB: locks |Id_|.
        SequoiaSession_->ClearSubtree(Path_);

        auto setter = TMapNodeSetter(SequoiaSession_.Get(), Path_, Id_);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&setter);

        SequoiaSession_->Commit(CellIdFromObjectId(Id_));

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
        // If the number of nodes in a subtree of certain depth it equal to the
        // limit, then we should fetch the next layer, so opaques can be set
        // correctly.
        while (std::ssize(nodeIdToChildren) <= responseSizeLimit + 1) {
            // This means we finished tree traversal.
            if (childrenLookupQueue.empty()) {
                break;
            }

            ++maxRetrievedDepth;

            std::vector<TFuture<std::vector<NRecords::TChildNode>>> asyncNextLayer;
            while (!childrenLookupQueue.empty()) {
                auto nodeId = childrenLookupQueue.front();
                childrenLookupQueue.pop();
                if (IsSequoiaCompositeNodeType(TypeFromId(nodeId))) {
                    asyncNextLayer.push_back(SequoiaSession_->FetchChildren(nodeId));
                }
            }

            // This means that we've already fetched all map-nodes.
            if (asyncNextLayer.empty()) {
                break;
            }

            // This should lead to a retry, but retries are not implemented in
            // Sequoia yet.
            // TODO(h0pless): update error once Sequoia retries are implemented.
            auto currentSubtreeLayerChildren = WaitFor(AllSucceeded(asyncNextLayer))
                .ValueOrThrow();

            for (const auto& children : currentSubtreeLayerChildren) {
                for (const auto& child : children) {
                    nodeIdToChildren[child.Key.ParentId].push_back(child);
                    nodeIdToChildren[child.ChildId] = {};
                    childrenLookupQueue.push(child.ChildId);
                }
            }
        }

        // Form a template.
        auto requestTemplate = TYPathProxy::Get();
        if (attributeFilter) {
            ToProto(requestTemplate->mutable_attributes(), attributeFilter);
        }

        // Find all nodes that need to be requested from master cells.
        std::vector<TNodeId> nodesToFetchFromMaster;
        for (const auto& [nodeId, children] : nodeIdToChildren) {
            auto nodeType = TypeFromId(nodeId);
            if (IsScalarType(nodeType) || attributeFilter) {
                nodesToFetchFromMaster.push_back(nodeId);
            }
        }

        auto vectorizedBatcher = TMasterYPathProxy::CreateGetBatcher(
            Bootstrap_->GetNativeRootClient(),
            requestTemplate,
            nodesToFetchFromMaster);
        auto nodeIdToRspOrError = WaitFor(vectorizedBatcher.Invoke())
            .ValueOrThrow();

        THashMap<TNodeId, TYPathProxy::TRspGetPtr> nodeIdToMasterResponse;
        for (auto [nodeId, rspOrError] : nodeIdToRspOrError) {
            if (!rspOrError.IsOK()) {
                // TODO(kvk1920): in case of race between Get(path) and
                // Create(path, force=true) for the same path we can get an
                // error "no such node". Retry is needed if a given path still
                // exists. Since retry mechanism is not implemented yet, this
                // will do for now.
                THROW_ERROR_EXCEPTION("Error getting requested information from master")
                    << rspOrError;
            }
            nodeIdToMasterResponse[nodeId] = rspOrError.Value();
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
        const NYPath::TYPath& path,
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
        const NYPath::TYPath& path,
        TReqSet* request,
        TRspSet* /*response*/,
        const TCtxSetPtr& context) override
    {
        // TODO(danilalexeev): Implement method _SetChild_ and bring out the common code with Create.
        auto recursive = request->recursive();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            recursive,
            request->force());

        auto unresolvedSuffix = TYPath("/" + path);
        auto destinationPath = Path_ + unresolvedSuffix;
        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        auto targetName = unresolvedSuffixTokens.back();
        unresolvedSuffixTokens.pop_back();

        if (!recursive && !unresolvedSuffixTokens.empty()) {
            ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
        }

        // NB: locks |Id_|.
        auto targetParentId = SequoiaSession_->CreateMapNodeChain(
            Path_,
            Id_,
            unresolvedSuffixTokens);

        auto builder = TTreeBuilder(SequoiaSession_.Get(), destinationPath, targetParentId);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&builder);

        SequoiaSession_->Commit(CellIdFromObjectId(Id_));

        context->Reply();
    }

    void RemoveRecursive(
        const NYPath::TYPath& path,
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

    auto unresolvedSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    if (auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        !unresolvedSuffixTokens.empty())
    {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
    }

    auto children = WaitFor(SequoiaSession_->FetchChildren(Id_))
        .ValueOrThrow();


    // Should be no-op.
    SequoiaSession_->Commit();

    response->set_value(BuildYsonStringFluently()
        .BeginList()
            .DoFor(children, [&] (TFluentList fluent, const auto& child) {
                fluent
                    .Item().Value(child.Key.ChildKey);
            })
        .EndList().ToString());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

TNodeProxyBasePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TSequoiaResolveResult resolveResult)
{
    auto type = TypeFromId(resolveResult.Id);
    ValidateSupportedSequoiaType(type);

    if (IsSequoiaCompositeNodeType(type)) {
        return New<TMapLikeNodeProxy>(bootstrap, std::move(session), std::move(resolveResult));
    } else {
        return New<TNodeProxy>(bootstrap, std::move(session), std::move(resolveResult));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
