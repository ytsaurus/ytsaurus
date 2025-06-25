#include "node_proxy.h"

#include "bootstrap.h"
#include "helpers.h"
#include "master_connector.h"
#include "node_proxy_base.h"
#include "path_resolver.h"
#include "sequoia_tree_visitor.h"
#include "sequoia_session.h"
#include "sequoia_tree_visitor.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>
#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/async_writer.h>
#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <stack>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NServer;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_SUPPORTS_METHOD(Get);
DECLARE_SUPPORTS_METHOD(Set);
DECLARE_SUPPORTS_METHOD(Remove);
DECLARE_SUPPORTS_METHOD(List);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

IMPLEMENT_SUPPORTS_METHOD(Get)
IMPLEMENT_SUPPORTS_METHOD(Set)
IMPLEMENT_SUPPORTS_METHOD(Remove)
IMPLEMENT_SUPPORTS_METHOD(List)

IMPLEMENT_SUPPORTS_METHOD_RESOLVE(
    Exists,
    {
        context->SetRequestInfo();
        Reply(context, /*exists*/ false);
    })

void TSupportsExists::ExistsAttribute(
    const TYPath& /*path*/,
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
    const TYPath& /*path*/,
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
    , public TSupportsList
{
public:
    TNodeProxy(
        IBootstrap* bootstrap,
        TSequoiaSessionPtr session,
        TSequoiaResolveResult resolveResult)
        : TNodeProxyBase(bootstrap, std::move(session))
        , Id_(resolveResult.Id)
        , Path_(resolveResult.Path)
        , ParentId_(resolveResult.ParentId)
        , ResolveResult_(std::move(resolveResult))
    {
        auto nodeType = TypeFromId(Id_);
        YT_VERIFY(
            ParentId_ ||
            nodeType == EObjectType::Scion ||
            nodeType == EObjectType::Link ||
            IsSnapshot());
    }

protected:
    const TNodeId Id_;
    const TAbsolutePath Path_;
    // Can be null only if |Id_| is a scion, Cypress link or snapshot branch.
    const TNodeId ParentId_;
    const TSequoiaResolveResult ResolveResult_;

    TSuppressableAccessTrackingOptions AccessTrackingOptions_;

    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, MultisetAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermission);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Lock);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Unlock);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch);

    // Used for cross-cell copy.
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, LockCopyDestination);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, LockCopySource);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, CalculateInheritedAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, AssembleTreeCopy);

    // COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, BeginCopy);

    void BeforeInvoke(const ISequoiaServiceContextPtr& context) override
    {
        AccessTrackingOptions_ = {
            .SuppressAccessTracking = GetSuppressAccessTracking(context->RequestHeader()),
            .SuppressModificationTracking = GetSuppressModificationTracking(context->RequestHeader()),
            .SuppressExpirationTimeoutRenewal = GetSuppressExpirationTimeoutRenewal(context->RequestHeader()),
        };
    }

    bool IsSnapshot() const
    {
        return ResolveResult_.IsSnapshot();
    }

    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Set);
        DISPATCH_YPATH_SERVICE_METHOD(Remove);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        DISPATCH_YPATH_SERVICE_METHOD(MultisetAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermission);
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);
        DISPATCH_YPATH_SERVICE_METHOD(Lock);
        DISPATCH_YPATH_SERVICE_METHOD(Unlock);
        DISPATCH_YPATH_SERVICE_METHOD(Alter);
        DISPATCH_YPATH_SERVICE_METHOD(LockCopyDestination);
        DISPATCH_YPATH_SERVICE_METHOD(LockCopySource);
        DISPATCH_YPATH_SERVICE_METHOD(CalculateInheritedAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(AssembleTreeCopy);

        DISPATCH_YPATH_SERVICE_METHOD(BeginCopy);

        return false;
    }

    TObjectServiceProxy CreateReadProxyForObject(TObjectId id)
    {
        return TObjectServiceProxy::FromDirectMasterChannel(
            GetNativeAuthenticatedClient()->GetMasterChannelOrThrow(EMasterChannelKind::Follower, CellTagFromId(id)));
    }

    void ValidateCreateOptions(const TReqCreate* request)
    {
        if (request->ignore_type_mismatch()) {
            THROW_ERROR_EXCEPTION("Create with \"ignore_type_mismatch\" flag is not supported in Sequoia yet");
        }
        if (request->lock_existing()) {
            THROW_ERROR_EXCEPTION("Create with \"lock_existing\" flag is not supported in Sequoia yet");
        }

        auto type = FromProto<EObjectType>(request->type());
        if (type == EObjectType::SequoiaMapNode) {
            THROW_ERROR_EXCEPTION("%Qlv is internal type and should not be used directly; use %Qlv instead",
                EObjectType::SequoiaMapNode,
                EObjectType::MapNode);
        }
    }

    void MaybeTouchCurrentNode(auto requestFactory, const auto& context)
    {
        // For mutable requests access tracking is conducted in transaction actions.
        YT_VERIFY(!IsRequestMutating(context->GetRequestHeader()));

        if (AccessTrackingOptions_.SuppressAccessTracking &&
            AccessTrackingOptions_.SuppressExpirationTimeoutRenewal)
        {
            return;
        }

        auto req = requestFactory(FromObjectId(Id_));
        SetTransactionId(req, SequoiaSession_->GetCurrentCypressTransactionId());
        SetAccessTrackingOptions(req, AccessTrackingOptions_);
        SetAllowResolveFromSequoiaObject(req, true);

        auto rspOrError = WaitFor(CreateReadProxyForObject(Id_).Execute(std::move(req)));
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Node touch failed")
                << TErrorAttribute("node_id", Id_)
                << rspOrError;
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
        SetAllowResolveFromSequoiaObject(reqGet, true);
        auto rspGet = WaitFor(CreateReadProxyForObject(Id_).Execute(reqGet))
            .ValueOrThrow();
        auto rootstockId = ConvertTo<TNodeId>(NYson::TYsonString(rspGet->value()));

        return SequoiaSession_->RemoveRootstock(rootstockId);
    }

    void AbortSequoiaSessionForLaterForwardingToMaster()
    {
        InvokeResult_ = EInvokeResult::ForwardToMaster;
        SequoiaSession_->Abort();
    }

    struct TSubtreeReplacementResult
    {
        //! The target node (or subtree) will be a child of this node.
        TNodeId TargetParentId;
        //! This node is used to determine Sequoia tx coordinator.
        /*!
         *  NB: It's not the same as |TargetParentId|. Let's consider recursive
         *  creation "create map_node //a/b/c --recursive" where "//a" is
         *  already exists. In this case //a is attachment point and //a/b is
         *  target's parent.
         */
        TNodeId AttachmentPointNodeId;
        //! The name of target node (or subtree root) to create.
        std::string TargetNodeKey;
    };

    //! Replaces subtree with (maybe empty) chain of map-nodes and shared-locks
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
        TRange<std::string> unresolvedSuffixTokens,
        bool force,
        std::vector<TCypressNodeDescriptor>* removedNodes = nullptr)
    {
        // Inplace.
        if (unresolvedSuffixTokens.Empty()) {
            YT_VERIFY(force);

            auto subtreeToRemove = SequoiaSession_->FetchSubtree(Path_);

            // Acquires shared lock on row in Sequoia table.
            SequoiaSession_->DetachAndRemoveSubtree(
                subtreeToRemove,
                ParentId_,
                /*detachInLatePrepare*/ true);

            if (removedNodes) {
                *removedNodes = std::move(subtreeToRemove.Nodes);
            }

            return {
                .TargetParentId = ParentId_,
                .AttachmentPointNodeId = ParentId_,
                .TargetNodeKey = Path_.GetBaseName(),
            };
        }

        if (!IsSequoiaCompositeNodeType(TypeFromId(Id_))) {
            ThrowCannotHaveChildren(Path_);
        }

        return {
            // NB: |TargetParentId| is locked in CreateMapNodeChain().
            .TargetParentId = SequoiaSession_->CreateMapNodeChain(
                Path_,
                Id_,
                unresolvedSuffixTokens.Slice(0, unresolvedSuffixTokens.Size() - 1),
                /*options*/ {}),
            .AttachmentPointNodeId = Id_,
            .TargetNodeKey = unresolvedSuffixTokens.Back(),
        };
    }

    void GetSelf(TReqGet* request, TRspGet* /*response*/, const TCtxGetPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();
        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        AbortSequoiaSessionForLaterForwardingToMaster();
    }

    void SetSelf(TReqSet* request, TRspSet* /*response*/, const TCtxSetPtr& context) override
    {
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            request->recursive(),
            force);

        SequoiaSession_->SetNode(Id_, NYson::TYsonString(request->value()), AccessTrackingOptions_);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
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
            auto subtree = SequoiaSession_->FetchSubtree(Path_);
            // Subtree must consist of at least its root.
            YT_VERIFY(!subtree.Nodes.empty());
            SequoiaSession_->DetachAndRemoveSubtree(
                subtree,
                ParentId_,
                /*detachInLatePrepare*/ true);
        } else if (IsSequoiaCompositeNodeType(TypeFromId(Id_)) && !SequoiaSession_->IsMapNodeEmpty(Id_)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::CannotRemoveNonemptyCompositeNode,
                "Cannot remove non-empty composite node");
        } else {
            SequoiaSession_->DetachAndRemoveSingleNode(Id_, Path_, ParentId_);
        }

        // Detaching child for subtree root should be done in late prepare.
        FinishSequoiaSessionAndReply(context, CellIdFromCellTag(subtreeRootCell), /*commitSession*/ true);
    }

    void ExistsSelf(
        TReqExists* /*request*/,
        TRspExists* /*response*/,
        const TCtxExistsPtr& context) override
    {
        context->SetRequestInfo();
        AbortSequoiaSessionForLaterForwardingToMaster();
    }

    void ExistsAttribute(
        const TYPath& /*path*/,
        TReqExists* /*request*/,
        TRspExists* /*response*/,
        const TCtxExistsPtr& context) override
    {
        context->SetRequestInfo();
        AbortSequoiaSessionForLaterForwardingToMaster();
    }

    void GetAttribute(
        const TYPath& /*path*/,
        TReqGet* /*request*/,
        TRspGet* /*response*/,
        const TCtxGetPtr& context) override
    {
        context->SetRequestInfo();
        AbortSequoiaSessionForLaterForwardingToMaster();
    }

    void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* /*response*/,
        const TCtxSetPtr& context) override
    {
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            request->recursive(),
            force);

        SequoiaSession_->SetNodeAttribute(
            Id_,
            TYPathBuf("/@" + path),
            TYsonString(request->value()),
            force,
            AccessTrackingOptions_);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
    }

    void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* /*response*/,
        const TCtxRemovePtr& context) override
    {
        auto force = request->force();

        context->SetRequestInfo("Recursive: %v, Force: %v",
            request->recursive(),
            force);

        SequoiaSession_->RemoveNodeAttribute(Id_, TYPathBuf("/@" + path), force);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
    }

    void ListAttribute(
        const TYPath& /*path*/,
        TReqList* request,
        TRspList* /*response*/,
        const TCtxListPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();


        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        AbortSequoiaSessionForLaterForwardingToMaster();
    }
};

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, MultisetAttributes)
{
    auto force = request->force();

    context->SetRequestInfo("KeyCount: %v, Force: %v",
        request->subrequests_size(),
        force);

    auto subrequests = FromProto<std::vector<TMultisetAttributesSubrequest>>(request->subrequests());

    auto targetPath = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    SequoiaSession_->MultisetNodeAttributes(
        Id_,
        targetPath,
        subrequests,
        force,
        AccessTrackingOptions_);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, GetBasicAttributes)
{
    context->SetRequestInfo();

    auto unresolvedSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);

    if (!unresolvedSuffixTokens.empty()) {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens.front());
    }

    AbortSequoiaSessionForLaterForwardingToMaster();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, CheckPermission)
{
    const auto& userName = request->user();
    auto permission = FromProto<EPermission>(request->permission());
    auto columns = request->has_columns()
        ? std::optional(FromProto<std::vector<std::string>>(request->columns().items()))
        : std::nullopt;
    auto vital = YT_OPTIONAL_FROM_PROTO(*request, vital, bool);
    bool ignoreSafeMode = request->ignore_safe_mode();

    context->SetRequestInfo("User: %v, Permission: %v, Columns: %v, Vital: %v, IgnoreSafeMode: %v",
        userName,
        permission,
        columns,
        vital,
        ignoreSafeMode);

    // TODO(babenko): implement

    response->set_action(ToProto(NSecurityClient::ESecurityAction::Allow));
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Fetch)
{
    context->SetRequestInfo();
    AbortSequoiaSessionForLaterForwardingToMaster();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Create)
{
    auto type = FromProto<EObjectType>(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto recursive = request->recursive();
    auto force = request->force();
    auto ignoreTypeMismatch = request->ignore_type_mismatch();
    auto hintId = FromProto<TNodeId>(request->hint_id());

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
        SequoiaSession_->GetCurrentCypressTransactionId());

    ValidateCreateOptions(request);

    // This alert can be safely removed since hintId is not used in this function.
    YT_LOG_ALERT_IF(hintId, "Hint ID was received on Cypress proxy (HintId: %v)", hintId);

    if (type == EObjectType::MapNode) {
        type = EObjectType::SequoiaMapNode;
    }

    auto explicitAttributes = request->has_node_attributes()
        ? NYTree::FromProto(request->node_attributes())
        : CreateEphemeralAttributes();

    if (type == EObjectType::Link) {
        auto targetPath = ValidateAndMakeYPath(
            explicitAttributes->Get<TRawYPath>(EInternedAttributeKey::TargetPath.Unintern()));
        ValidateLinkNodeCreation(
            SequoiaSession_,
            targetPath,
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

    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
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

        ToProto(response->mutable_node_id(), Id_);
        response->set_cell_tag(ToProto(CellTagFromId(Id_)));

        context->SetResponseInfo("ExistingNodeId: %v", Id_);

        if (lockExisting) {
            SequoiaSession_->LockNodeImplicitly(
                Id_,
                ELockMode::Exclusive,
                /*childKey*/ std::nullopt,
                /*attributeKey*/ std::nullopt);
        }

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), lockExisting);
        return;
    }

    if (!recursive && unresolvedSuffixTokens.size() > 1) {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
    }

    auto [targetParentNodeId, attachmentPointNodeId, targetKey] = ReplaceSubtreeWithMapNodeChain(
        unresolvedSuffixTokens,
        force);
    auto targetNodePath = JoinNestedNodesToPath(Path_, unresolvedSuffixTokens);

    auto createdNodeId = SequoiaSession_->CreateNode(
        type,
        targetNodePath,
        explicitAttributes.Get(),
        targetParentNodeId,
        /*options*/ {});

    ToProto(response->mutable_node_id(), createdNodeId);
    response->set_cell_tag(ToProto(CellTagFromId(createdNodeId)));

    // TODO(h0pless): Add account info here, currently impossible to integrate properly due to the fact
    // that there is no such attribute stored in Sequoia dynamic tables.
    context->SetResponseInfo("NodeId: %v, CellTag: %v",
        createdNodeId,
        CellTagFromId(createdNodeId));

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(attachmentPointNodeId), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Copy)
{
    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    if (ypathExt.additional_paths_size() != 1) {
        THROW_ERROR_EXCEPTION("Invalid number of additional paths");
    }

    auto originalSourcePath = ValidateAndMakeYPath(TRawYPath(ypathExt.additional_paths(0)));
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
        SequoiaSession_->GetCurrentCypressTransactionId(),
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

    if (!ignoreExisting && lockExisting) {
        THROW_ERROR_EXCEPTION("Cannot specify \"lock_existing\" without \"ignore_existing\"");
    }

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (ignoreExisting && options.Mode == ENodeCloneMode::Move) {
        // This practically never happens. Maybe consider adding YT_VERIFY here.
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_existing\" for move operation");
    }

    Visit(GetRootDesignator(originalSourcePath).first,
        [&] (TObjectId objectId) {
            if (auto type = TypeFromId(objectId); !IsVersionedType(type)) {
                THROW_ERROR_EXCEPTION("Path %v points to a nonversioned %Qlv object instead of a node",
                    originalSourcePath,
                    type);
            }
        },
        [] (TSlashRootDesignatorTag) {});

    auto sourceResolveResult = ResolvePath(
        SequoiaSession_,
        originalSourcePath,
        /*pathIsAdditional*/ true,
        context->GetService(),
        "Copy");

    const auto* resolvedSource = std::get_if<TSequoiaResolveResult>(&sourceResolveResult);
    THROW_ERROR_EXCEPTION_IF(!resolvedSource,
        NObjectClient::EErrorCode::CrossCellAdditionalPath,
        "Request involves Sequoia path %v and Cypress additional path %v",
        ypathExt.original_target_path(),
        originalSourcePath);

    // TODO(h0pless): Support acl preservation. It has to be done both here and in master.
    if (options.PreserveAcl) {
        THROW_ERROR_EXCEPTION("Copy with \"preserve_acl\" flag is not supported in Sequoia yet");
    }

    // TODO(h0pless): This might not be the best solution in a long run, but it'll work for now.
    // Clarification: we need to convert scion into Sequoia map node, currently we can't do that.
    if (TypeFromId(resolvedSource->Id) == EObjectType::Scion) {
        THROW_ERROR_EXCEPTION("Scion cannot be cloned");
    }

    // NB: Rewriting in case there were links in the original source path.
    const auto& sourceRootPath = resolvedSource->Path;
    if (auto sourceUnresolvedSuffix = resolvedSource->UnresolvedSuffix;
        !sourceUnresolvedSuffix.empty())
    {
        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(sourceUnresolvedSuffix);
        ThrowNoSuchChild(sourceRootPath, unresolvedSuffixTokens[0]);
    }

    // NB: From now on, all links are resolved and no path contains links
    // so we can just compare paths here.
    if (Path_ == sourceRootPath) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to itself");
    }
    if (Path_.Underlying().StartsWith(sourceRootPath.Underlying())) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to its descendant");
    }

    // Validate there are no duplicate or missing destination nodes.
    auto unresolvedDestinationSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    auto destinationSuffixDirectoryTokens = TokenizeUnresolvedSuffix(unresolvedDestinationSuffix);
    if (destinationSuffixDirectoryTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        if (lockExisting) {
            SequoiaSession_->LockNodeImplicitly(
                Id_,
                ELockMode::Exclusive,
                /*childKey*/ std::nullopt,
                /*attributeKey*/ std::nullopt);
        }

        ToProto(response->mutable_node_id(), Id_);

        context->SetResponseInfo("ExistingNodeId: %v", Id_);
        // TODO(danilalexeev): Lock the source node's row in Sequoia tables to ensure correct access tracking.
        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), lockExisting);
        return;
    }

    if (!recursive && std::ssize(destinationSuffixDirectoryTokens) > 1) {
        ThrowNoSuchChild(Path_, destinationSuffixDirectoryTokens[0]);
    }

    std::vector<TCypressNodeDescriptor> removedNodes;
    auto [destinationParentId, attachmentPointNodeId, targetKey] = ReplaceSubtreeWithMapNodeChain(
        destinationSuffixDirectoryTokens,
        force,
        &removedNodes);

    auto nodesToCopy = SequoiaSession_->FetchSubtree(sourceRootPath);

    // Select returns sorted entries and destination subtree cannot include source subtree.
    // Thus to check that subtrees don't overlap it's enough to check source root with
    // first and last elements of the destination subtree.
    if (options.Mode == ENodeCloneMode::Move &&
        (removedNodes.empty() ||
         sourceRootPath < removedNodes.front().Path ||
         removedNodes.back().Path < sourceRootPath))
    {
        auto sourceParentId = resolvedSource->ParentId;
        // Since source is not a scion (because they cannot be copied) it has at
        // least one ancestor in Sequoia.
        YT_VERIFY(sourceParentId);

        SequoiaSession_->DetachAndRemoveSubtree(
            nodesToCopy,
            sourceParentId,
            /*detachInLatePrepare*/ false);
    }

    auto destinationRootPath = JoinNestedNodesToPath(Path_, destinationSuffixDirectoryTokens);
    auto destinationId = SequoiaSession_->CopySubtree(
        nodesToCopy,
        destinationRootPath,
        destinationParentId,
        options);

    ToProto(response->mutable_node_id(), destinationId);

    context->SetResponseInfo("NodeId: %v", destinationId);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(attachmentPointNodeId), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Unlock)
{
    context->SetRequestInfo();

    SequoiaSession_->ValidateTransactionPresence();

    if (auto unresolvedSuffix = GetRequestTargetYPath(context->RequestHeader()); !unresolvedSuffix.empty()) {
        NYPath::TTokenizer tokenizer(unresolvedSuffix);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::Literal) {
            ThrowNoSuchChild(Path_, tokenizer.GetLiteralValue());
        } else {
            ThrowMethodNotSupported(context->GetMethod());
        }
    }

    SequoiaSession_->UnlockNode(Id_, IsSnapshot());

    context->SetResponseInfo();

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Alter)
{
    auto unresolvedSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
    if (!unresolvedSuffixTokens.empty()) {
        ThrowNoSuchChild(Path_, unresolvedSuffixTokens.front());
    }

    context->SetRequestInfo("Dynamic: %v, UpstreamReplicaId: %v, SchemaModification: %v, ReplicationProgress: %v, SchemaId: %v",
        YT_OPTIONAL_FROM_PROTO(*request, dynamic),
        YT_OPTIONAL_FROM_PROTO(*request, upstream_replica_id, NTabletClient::TTableReplicaId),
        YT_OPTIONAL_FROM_PROTO(*request, schema_modification, NTableClient::ETableSchemaModification),
        YT_OPTIONAL_FROM_PROTO(*request, replication_progress, NChaosClient::TReplicationProgress),
        YT_OPTIONAL_FROM_PROTO(*request, schema_id, TObjectId));

    AbortSequoiaSessionForLaterForwardingToMaster();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, Lock)
{
    SequoiaSession_->ValidateTransactionPresence();

    auto mode = FromProto<ELockMode>(request->mode());
    auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
    auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);
    auto timestamp = request->timestamp();
    auto waitable = request->waitable();

    // TODO(kvk1920): move TLockKey to /yt/yt/server/lib/sequoia to avoid code
    // duplication.
    context->SetRequestInfo("Mode: %v, Key: %v, Waitable: %v",
        mode,
        MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
            if (childKey) {
                builder->AppendFormat("Child[%v]", *childKey);
            } else if (attributeKey) {
                builder->AppendFormat("Attribute[%v]", *attributeKey);
            } else {
                builder->AppendString("None");
            }
        }),
        waitable);

    if (auto unresolvedSuffix = GetRequestTargetYPath(context->RequestHeader()); !unresolvedSuffix.empty()) {
        NYPath::TTokenizer tokenizer(unresolvedSuffix);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::Literal) {
            ThrowNoSuchChild(Path_, tokenizer.GetLiteralValue());
        } else {
            ThrowMethodNotSupported(context->GetMethod());
        }
    }

    if (!SequoiaSession_->GetCurrentCypressTransactionId()) {
        THROW_ERROR_EXCEPTION("Operation cannot be performed outside of a transaction");
    }

    CheckLockRequest(mode, childKey, attributeKey)
        .ThrowOnError();

    auto lockId = SequoiaSession_->LockNodeExplicitly(Id_, mode, childKey, attributeKey, timestamp, waitable);

    // TODO(cherepashka): add response for `Lock` into sequoia response keeper via dataless write rows.
    SequoiaSession_->Commit(CellIdFromObjectId(Id_));

    const auto& client = GetNativeAuthenticatedClient();

    const auto& stateAttribute = EInternedAttributeKey::State.Unintern();
    auto asyncLockAcquired = waitable
        ? FetchSingleObject(
            client,
            TVersionedObjectId{lockId},
            TAttributeFilter({stateAttribute}))
            .Apply(BIND([&] (const INodePtr& rsp) {
                return rsp->Attributes().Get<ELockState>(stateAttribute) == ELockState::Acquired;
            }))
        : MakeFuture<bool>(true);

    const auto& externalCellTagAttribute = EInternedAttributeKey::ExternalCellTag.Unintern();
    const auto& revisionAttribute = EInternedAttributeKey::Revision.Unintern();

    auto asyncNode = FetchSingleObject(
        client,
        {Id_, SequoiaSession_->GetCurrentCypressTransactionId()},
        TAttributeFilter({externalCellTagAttribute, revisionAttribute}));

    auto nodeLocked = WaitForFast(asyncLockAcquired)
        .ValueOrThrow();
    auto node = WaitFor(asyncNode)
        .ValueOrThrow();

    auto revision = nodeLocked
        ? node->Attributes().Get<NHydra::TRevision>(revisionAttribute)
        : NHydra::NullRevision;
    auto nativeCellTag = CellTagFromId(Id_);
    auto externalCellTag = node
        ->Attributes()
        .Find<TCellTag>(externalCellTagAttribute)
        .value_or(nativeCellTag);

    auto externalTransactionId = externalCellTag == nativeCellTag
        ? SequoiaSession_->GetCurrentCypressTransactionId()
        : MakeExternalizedTransactionId(SequoiaSession_->GetCurrentCypressTransactionId(), nativeCellTag);

    ToProto(response->mutable_lock_id(), lockId);
    ToProto(response->mutable_node_id(), Id_);
    ToProto(response->mutable_external_transaction_id(), externalTransactionId);
    response->set_external_cell_tag(ToProto(externalCellTag));
    response->set_revision(ToProto(revision));

    context->SetResponseInfo("LockId: %v, ExternalCellTag: %v, ExternalTransactionId: %v, Revision: %x",
        lockId,
        externalCellTag,
        externalTransactionId,
        revision);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, LockCopyDestination)
{
    SequoiaSession_->ValidateTransactionPresence();

    auto force = request->force();
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto preserveAcl = request->preserve_acl();
    auto recursive = request->recursive();

    auto inplace = request->inplace();
    auto targetPath = GetRequestTargetYPath(context->RequestHeader());
    auto replace = targetPath.empty();

    context->SetRequestInfo(
        "Force: %v, IgnoreExisting: %v, LockExisting: %v, Replace: %v, "
        "Inplace: %v, PreserveAcl: %v, Recursive: %v, TransactionId: %v",
        force,
        ignoreExisting,
        lockExisting,
        replace,
        inplace,
        preserveAcl,
        recursive,
        SequoiaSession_->GetCurrentCypressTransactionId());

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (replace && !force && !inplace) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        if (lockExisting) {
            SequoiaSession_->LockNodeImplicitly(
                Id_,
                ELockMode::Exclusive,
                /*childKey*/ std::nullopt,
                /*attributeKey*/ std::nullopt);
        }

        ToProto(response->mutable_existing_node_id(), Id_);
        context->SetResponseInfo("ExistingNodeId: %v", Id_);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), lockExisting);
        return;
    }

    auto targetPathDirectoryTokens = TokenizeUnresolvedSuffix(targetPath);
    if (!recursive && std::ssize(targetPathDirectoryTokens) > 1) {
        ThrowNoSuchChild(Path_, targetPathDirectoryTokens[0]);
    }

    if (!replace && !IsSequoiaCompositeNodeType(TypeFromId(Id_))) {
        ThrowCannotHaveChildren(Path_);
    }

    // The node inside which the cloned node must be created. Usually it's the current one.
    auto parentNodeId = Id_;
    if (!inplace) {
        std::string childNodeKey;
        if (replace) {
            if (!ParentId_) {
                ThrowCannotReplaceNode(Path_);
            }

            parentNodeId = ParentId_;
            childNodeKey = Path_.GetBaseName();
        } else {
            childNodeKey = targetPathDirectoryTokens[0];
        }

        // This lock ensures that both parent node and child node won't change before AssembleTreeCopy is called.
        // For inplace copy this is not needed, since the node is freshly created under current transaction.
        SequoiaSession_->LockNodeImplicitly(
            parentNodeId,
            ELockMode::Shared,
            childNodeKey,
            /*attributeKey*/ std::nullopt);
    }

    // TODO(h0pless): Add ACL validation here. See LockCopyDestination in master.

    // TODO(h0pless): Fetch accounts and inheritable attributes from Sequoia tables once those are replicated.
    // For now Get request to master is good enough. Please note, that it's technically racy.

    auto client = GetNativeAuthenticatedClient();
    const auto& accountIdAttribute = EInternedAttributeKey::AccountId.Unintern();
    const auto& effectiveInheritableAttributes = EInternedAttributeKey::EffectiveInheritableAttributes.Unintern();
    auto asyncNode = FetchSingleObject(
        client,
        {parentNodeId, SequoiaSession_->GetCurrentCypressTransactionId()},
        TAttributeFilter({accountIdAttribute, effectiveInheritableAttributes}));

    auto node = WaitFor(asyncNode)
        .ValueOrThrow();

    auto accountId = node
        ->Attributes()
        .Get<TAccountId>(accountIdAttribute);
    auto effectiveInheritableAttributesYson = node
        ->Attributes()
        .GetYson(effectiveInheritableAttributes);
    auto effectiveInheritableAttributesDictionary = ConvertToAttributes(effectiveInheritableAttributesYson);

    // TODO(h0pless): Maybe create all nodes all the way up to PARENT node? See LockCopyDestination in master.
    // I think both should be done simultaneously to facilitate the transition.

    auto nativeCellTag = CellTagFromId(Id_);
    context->SetResponseInfo("NativeCellTag: %v, AccountId: %v, EffectiveInheritedAttributes: %v",
        nativeCellTag,
        accountId,
        effectiveInheritableAttributesDictionary->ListPairs());

    response->set_sequoia_destination(true);
    response->set_native_cell_tag(nativeCellTag.Underlying());
    ToProto(response->mutable_account_id(), accountId);
    ToProto(response->mutable_effective_inheritable_attributes(), *effectiveInheritableAttributesDictionary);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(parentNodeId), !inplace);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, LockCopySource)
{
    SequoiaSession_->ValidateTransactionPresence();

    auto mode = FromProto<ENodeCloneMode>(request->mode());

    context->SetRequestInfo("Mode: %v, Transaction: %v",
        mode,
        SequoiaSession_->GetCurrentCypressTransactionId());

    // TODO(h0pless): Add ACL validation here. See LockCopySource in master.

    const auto& connector = Bootstrap_->GetMasterConnector();
    auto maxSubtreeSize = connector->GetMaxCopiableSubtreeSize();

    i64 subtreeSize = 0;
    auto nodesToCopy = SequoiaSession_->FetchSubtree(Path_);
    if (std::ssize(nodesToCopy.Nodes) > maxSubtreeSize) {
        THROW_ERROR_EXCEPTION("Subtree is too large for cross-cell copy")
            << TErrorAttribute("subtree_size", subtreeSize)
            << TErrorAttribute("max_subtree_size", maxSubtreeSize);
    }

    auto lockMode = mode == ENodeCloneMode::Copy ? ELockMode::Snapshot : ELockMode::Exclusive;
    std::vector<TFuture<std::vector<TCypressChildDescriptor>>> asyncNodesInfo;
    for (const auto& currentNode : nodesToCopy.Nodes) {
        SequoiaSession_->LockNodeImplicitly(
            currentNode.Id,
            lockMode,
            /*childKey*/ std::nullopt,
            /*attributeKey*/ std::nullopt);

        if (IsSequoiaCompositeNodeType(TypeFromId(currentNode.Id))) {
            // This is suboptimal. All the information that might be needed here is already present, and this slows the code
            // down for the sake of convinience.
            // TODO(h0pless): Implement tree traverser once ACLs are implemented.
            asyncNodesInfo.push_back(SequoiaSession_->FetchChildren(currentNode.Id));
        }
    }

    auto nodesInfo = WaitFor(AllSucceeded(asyncNodesInfo))
        .ValueOrThrow();

    // It's important that parent node is saved before its children are.
    for (const auto& currentNodeInfo : nodesInfo) {
        if (currentNodeInfo.empty()) {
            // Node has no children, saving up on the response space.
            continue;
        }

        auto* nodeIdToChildrenEntry = response->add_node_id_to_children();

        auto parentId = currentNodeInfo[0].ParentId;
        ToProto(nodeIdToChildrenEntry->mutable_node_id(), parentId);

        for (const auto& currentChildInfo : currentNodeInfo) {
            auto* childEntry = nodeIdToChildrenEntry->add_children();
            childEntry->set_key(currentChildInfo.ChildKey);
            ToProto(childEntry->mutable_id(), currentChildInfo.ChildId);
        }
    }

    auto reign = connector->GetMasterReign();
    response->set_version(reign);
    ToProto(response->mutable_root_node_id(), Id_);

    context->SetResponseInfo("NodeCount: %v",
        response->node_id_to_children_size());

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, CalculateInheritedAttributes)
{
    SequoiaSession_->ValidateTransactionPresence();

    auto dstInheritedAttributes = NYTree::FromProto(request->dst_attributes());
    auto shouldCalculateInheritedAttributes = false;

    context->SetRequestInfo("DestinationInheritedAttributes: %v, ShouldCalculateInheritedAttributes: %v",
        dstInheritedAttributes->ListPairs(),
        shouldCalculateInheritedAttributes);

    // TODO(h0pless): Actually implement this functionality once inheritable attributes are replicated to Sequoia tables.
    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ shouldCalculateInheritedAttributes);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, AssembleTreeCopy)
{
    if (!request->sequoia_destination()) {
        InvokeResult_ = EInvokeResult::ForwardToMaster;
        return;
    }

    SequoiaSession_->ValidateTransactionPresence();

    auto force = request->force();
    auto inplace = request->inplace();
    auto preserveModificationTime = request->preserve_modification_time();
    auto preserveAcl = request->preserve_acl();
    context->SetIncrementalRequestInfo(
        "RootNodeId: %v, Force: %v, Inplace: %v, PreserveModificationTime: %v, PreserveAcl: %v",
        TVersionedNodeId{Id_, SequoiaSession_->GetCurrentCypressTransactionId()},
        force,
        inplace,
        preserveModificationTime,
        preserveAcl);

    auto rootNodeId = FromProto<TNodeId>(request->root_node_id());

    auto unresolvedDestinationSuffix = TYPath(GetRequestTargetYPath(context->GetRequestHeader()));
    auto destinationSuffixDirectoryTokens = TokenizeUnresolvedSuffix(unresolvedDestinationSuffix);
    // NB: Check for recursive creation was done during LockCopyDestination.
    auto [destinationParentId, attachmentPointNodeId, targetKey] = ReplaceSubtreeWithMapNodeChain(
        destinationSuffixDirectoryTokens,
        force);

    // Sanity checks.
    YT_LOG_ALERT_IF(
        request->node_id_to_children_size() == 0,
        "Empty list received when attempting to assemble tree copy");
    YT_LOG_ALERT_IF(
        rootNodeId != FromProto<TNodeId>(request->node_id_to_children()[0].node_id()),
        "Received malformed request to assemble tree copy (RootNodeId: %v, FirstElementInMapping: %v)",
        rootNodeId,
        FromProto<TNodeId>(request->node_id_to_children()[0].node_id()));

    THashMap<TNodeId, std::vector<TCypressChildDescriptor>> nodeIdToChildrenInfo;
    for (const auto& nodeIdToChild : request->node_id_to_children()) {
        auto nodeId = FromProto<TNodeId>(nodeIdToChild.node_id());
        nodeIdToChildrenInfo[nodeId].reserve(nodeIdToChild.children_size());
        for (const auto& child : nodeIdToChild.children()) {
            auto childId = FromProto<TNodeId>(child.id());
            nodeIdToChildrenInfo[nodeId].push_back({
                .ParentId = nodeId,
                .ChildId = childId,
                .ChildKey = child.key(),
            });
        }
    }

    auto destinationRootPath = PathJoin(
        Path_,
        TRelativePath::MakeCanonicalPathOrThrow(unresolvedDestinationSuffix));
    SequoiaSession_->AssembleTreeCopy(
        rootNodeId,
        destinationParentId,
        destinationRootPath,
        preserveAcl,
        preserveModificationTime,
        std::move(nodeIdToChildrenInfo));

    context->SetResponseInfo("NodeId: %v", rootNodeId);

    ToProto(response->mutable_node_id(), rootNodeId);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(attachmentPointNodeId), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TNodeProxy, BeginCopy)
{
    context->SetRequestInfo("Mode: %v",
        FromProto<ENodeCloneMode>(request->mode()));

    THROW_ERROR_EXCEPTION(
        NObjectClient::EErrorCode::BeginCopyDeprecated,
        "BeginCopy verb is deprecated");
}

////////////////////////////////////////////////////////////////////////////////

//! Orchid and document nodes are opaque from Sequoia point of view: resolve
//! into them cannot be done via Sequoia tables only. For such nodes every
//! non-mutating and recursive mutating requests have to be forwarded to master.
class TOpaqueNodeProxy
    : public TNodeProxy
{
public:
    using TNodeProxy::TNodeProxy;

private:
    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        if (IsRequestMutating(context->RequestHeader())) {
            return TNodeProxy::DoInvoke(context);
        }

        context->SetRequestInfo("Path: %v%v, TargetObjectId: %v",
            Path_,
            GetRequestTargetYPath(context->RequestHeader()),
            TVersionedNodeId{Id_, SequoiaSession_->GetCurrentCypressTransactionId()});
        AbortSequoiaSessionForLaterForwardingToMaster();
        return true;
    }

    void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* /*response*/,
        const TCtxSetPtr& context) override
    {
        context->SetRequestInfo("TargetNodeId: %v, PathSuffix: %v, Force: %v",
            Id_,
            path,
            request->force());
        AbortSequoiaSessionForLaterForwardingToMaster();
    }

    void RemoveRecursive(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* /*response*/,
        const TCtxRemovePtr& context) override
    {
        context->SetRequestInfo("TargetNodeId: %v, PathSuffix: %v, Force: %v, Recursive: %v",
            Id_,
            path,
            request->force(),
            request->recursive());

        AbortSequoiaSessionForLaterForwardingToMaster();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapLikeNodeProxy
    : public TNodeProxy
{
public:
    using TNodeProxy::TNodeProxy;

private:
    // TODO(h0pless): This class can be moved to helpers.
    // It only uses Owner_->SequoiaSession_, it's safe to change owner's type from proxy to transaction.
    //! This class consumes YSON, builds tree and attaches it to its parent.
    class TTreeBuilder
        : public NYson::TForwardingYsonConsumer
    {
    public:
        // NB: If #subtreePath is "//a/b/c" then #parentId is the ID of "//a/b".
        explicit TTreeBuilder(
            TSequoiaSession* session,
            TAbsolutePath subtreePath,
            TNodeId parentId,
            TSuppressableAccessTrackingOptions options)
            : UncaughtExceptions_(std::uncaught_exceptions())
            , Session_(session)
            , AccessTrackingOptions_(std::move(options))
            , CurrentPath_(std::move(subtreePath))
        {
            YT_VERIFY(Session_);

            CurrentAncestors_.push(parentId);
        }

        ~TTreeBuilder()
        {
            if (std::uncaught_exceptions() == UncaughtExceptions_) {
                // Failure here means that the tree is not fully constructed yet.
                YT_VERIFY(CurrentAncestors_.size() == 1);
            }
        }

        void OnMyKeyedItem(TYPathBuf key) override
        {
            CurrentPath_.Join(
                TRelativePath::UnsafeMakeCanonicalPath(TYPath(TRelativePath::Separator) + key));
        }

        void OnMyBeginMap() override
        {
            auto nodeId = CreateNode(EObjectType::SequoiaMapNode);
            CurrentAncestors_.push(nodeId);
        }

        void OnMyEndMap() override
        {
            CurrentPath_.RemoveLastSegment();
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

        void OnMyBeginAttributes() override
        {
            YT_ASSERT(!AttributeConsumer_);
            Attributes_ = CreateEphemeralAttributes();
            AttributeConsumer_ = std::make_unique<TAttributeConsumer>(Attributes_.Get());
            Forward(AttributeConsumer_.get(), nullptr, NYson::EYsonType::MapFragment);
        }

        void OnMyEndAttributes() override
        {
            AttributeConsumer_.reset();
            YT_ASSERT(Attributes_);
        }

    private:
        const int UncaughtExceptions_;
        TSequoiaSession* const Session_;
        const TSuppressableAccessTrackingOptions AccessTrackingOptions_;

        std::stack<TNodeId, std::vector<TNodeId>> CurrentAncestors_;
        TAbsolutePath CurrentPath_;
        std::unique_ptr<TAttributeConsumer> AttributeConsumer_;
        IAttributeDictionaryPtr Attributes_;

        template <class T>
        void CreateNonCompositeNodeAndPopItsKey(EObjectType type, const T& value)
        {
            auto nodeId = CreateNode(type);
            Session_->SetNode(nodeId, NYson::ConvertToYsonString(value), AccessTrackingOptions_);

            CurrentPath_.RemoveLastSegment();
        }

        TNodeId CreateNode(EObjectType type)
        {
            auto nodeId = Session_->CreateNode(
                type,
                CurrentPath_,
                Attributes_.Get(),
                CurrentAncestors_.top(),
                AccessTrackingOptions_);
            Attributes_.Reset();

            return nodeId;
        }
    };

    class TMapNodeSetter
        : public TTypedConsumer
    {
    public:
        TMapNodeSetter(
            TSequoiaSession* session,
            TAbsolutePath path,
            TNodeId nodeId,
            TSuppressableAccessTrackingOptions options)
            : Session_(session)
            , Path_(std::move(path))
            , Id_(nodeId)
            , AccessTrackingOptions_(std::move(options))
        {
            YT_VERIFY(Session_);
            YT_VERIFY(IsSequoiaCompositeNodeType(TypeFromId(nodeId)));
        }

        void OnMyBeginAttributes() override
        {
            YT_ASSERT(!AttributeConsumer_);
            Attributes_ = CreateEphemeralAttributes();
            AttributeConsumer_ = std::make_unique<TAttributeConsumer>(Attributes_.Get());
            Forward(AttributeConsumer_.get(), nullptr, NYson::EYsonType::MapFragment);
        }

        void OnMyEndAttributes() override
        {
            AttributeConsumer_.reset();

            std::vector<TMultisetAttributesSubrequest> subrequests;
            for (auto& [key, value] : Attributes_->ListPairs()) {
                subrequests.push_back({
                    .AttributeKey = std::move(key),
                    .Value = std::move(value),
                });
            }
            Attributes_.Reset();

            Session_->MultisetNodeAttributes(
                Id_,
                TYPathBuf("/@"),
                subrequests,
                /*force*/ false,
                AccessTrackingOptions_);
        }

    private:
        TSequoiaSession* const Session_;
        const TAbsolutePath Path_;
        const TNodeId Id_;
        const TSuppressableAccessTrackingOptions AccessTrackingOptions_;

        std::optional<TTreeBuilder> SubtreeBuilderHolder_;
        std::unique_ptr<TAttributeConsumer> AttributeConsumer_;
        IAttributeDictionaryPtr Attributes_;

        void OnMyKeyedItem(TYPathBuf key) override
        {
            YT_ASSERT(!SubtreeBuilderHolder_.has_value());

            auto subtreeRootPath = PathJoin(
                Path_,
                TRelativePath::UnsafeMakeCanonicalPath(TYPath(TRelativePath::Separator) + key));

            auto& builder = SubtreeBuilderHolder_.emplace(
                Session_,
                std::move(subtreeRootPath),
                Id_,
                AccessTrackingOptions_);
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
        SequoiaSession_->ClearSubtree(Path_, AccessTrackingOptions_);

        auto setter = TMapNodeSetter(SequoiaSession_.Get(), Path_, Id_, AccessTrackingOptions_);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&setter);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

        // NB: This is an arbitrary value, it can be freely changed.
        // TODO(h0pless): Think about moving global limit to dynamic config.
        i64 responseSizeLimit = limit.value_or(100'000);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        if (limit && limit < 0) {
            THROW_ERROR_EXCEPTION("Limit is negative")
                << TErrorAttribute("limit", limit);
        }

        // Fetch nodes from child nodes table.
        std::queue<TNodeId> childrenLookupQueue;
        childrenLookupQueue.push(Id_);

        THashMap<TNodeId, std::vector<TCypressChildDescriptor>> nodeIdToChildren;
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

            std::vector<TFuture<std::vector<TCypressChildDescriptor>>> asyncNextLayer;
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

            //NB: An error here will lead to a retry of the whole request.
            auto currentSubtreeLayerChildren = WaitFor(AllSucceeded(asyncNextLayer))
                .ValueOrThrow();

            for (const auto& children : currentSubtreeLayerChildren) {
                for (const auto& child : children) {
                    nodeIdToChildren[child.ParentId].push_back(child);
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
        SetSuppressAccessTracking(requestTemplate, true);
        SetSuppressExpirationTimeoutRenewal(requestTemplate, true);

        // Find all nodes that need to be requested from master cells.
        std::vector<TNodeId> nodesToFetchFromMaster;
        for (const auto& [nodeId, children] : nodeIdToChildren) {
            auto nodeType = TypeFromId(nodeId);
            if (IsScalarType(nodeType) || attributeFilter) {
                nodesToFetchFromMaster.push_back(nodeId);
            }
        }

        auto vectorizedBatcher = TMasterYPathProxy::CreateGetBatcher(
            GetNativeAuthenticatedClient(),
            requestTemplate,
            nodesToFetchFromMaster,
            SequoiaSession_->GetCurrentCypressTransactionId());
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

        MaybeTouchCurrentNode(TYPathProxy::Get, context);
        // Should not throw after this point.

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

        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

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

    void ListRecursive(
        const TYPath& path,
        TReqList* request,
        TRspList* /*response*/,
        const TCtxListPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        // See |TMapLikeNodeProxy::GetRecursive|.
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

        auto unresolvedSuffixTokens = TokenizeUnresolvedSuffix("/" + path);
        auto destinationPath = JoinNestedNodesToPath(Path_, unresolvedSuffixTokens);
        auto targetName = unresolvedSuffixTokens.back();
        unresolvedSuffixTokens.pop_back();

        if (!recursive && !unresolvedSuffixTokens.empty()) {
            ThrowNoSuchChild(Path_, unresolvedSuffixTokens[0]);
        }

        // NB: locks |Id_|.
        auto targetParentId = SequoiaSession_->CreateMapNodeChain(
            Path_,
            Id_,
            unresolvedSuffixTokens,
            AccessTrackingOptions_);

        auto builder = TTreeBuilder(SequoiaSession_.Get(), destinationPath, targetParentId, AccessTrackingOptions_);
        auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
        producer.Run(&builder);

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
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

        if (tokenizer.GetType() == NYPath::ETokenType::Asterisk) {
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            SequoiaSession_->ClearSubtree(Path_, AccessTrackingOptions_);
            FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ true);
            return;
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);

        // There is no composite node type other than Sequoia map node. If we
        // have unresolved suffix it can be either attribute or non-existent child.
        // Flag force was specifically designed to ignore this error.
        if (!force) {
            ThrowNoSuchChild(Path_, tokenizer.GetLiteralValue());
        }

        FinishSequoiaSessionAndReply(context, CellIdFromObjectId(Id_), /*commitSession*/ false);
    }

    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        auto attributeFilter = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto limit = YT_OPTIONAL_FROM_PROTO(*request, limit);

        context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
            limit,
            attributeFilter);

        if (limit && limit < 0) {
            THROW_ERROR_EXCEPTION("Limit is negative")
                << TErrorAttribute("limit", limit);
        }

        TAsyncYsonWriter writer;

        auto children = WaitFor(SequoiaSession_->FetchChildren(Id_))
            .ValueOrThrow();

        if (limit && std::ssize(children) > limit) {
            children.resize(*limit);

            writer.OnBeginAttributes();
            writer.OnKeyedItem("incomplete");
            writer.OnBooleanScalar(true);
            writer.OnEndAttributes();
        }

        THashMap<TNodeId, IAttributeDictionaryPtr> nodeIdToAttributes;
        if (attributeFilter) {
            // Form a template.
            auto requestTemplate = TYPathProxy::Get("/@");
            ToProto(requestTemplate->mutable_attributes(), attributeFilter);

            std::vector<TNodeId> childNodeIds;
            for (const auto& child : children) {
                childNodeIds.push_back(child.ChildId);
            }

            auto vectorizedBatcher = TMasterYPathProxy::CreateGetBatcher(
                GetNativeAuthenticatedClient(),
                requestTemplate,
                childNodeIds,
                SequoiaSession_->GetCurrentCypressTransactionId());

            auto nodeIdToRspOrError = WaitFor(vectorizedBatcher.Invoke())
                .ValueOrThrow();

            for (auto [nodeId, rspOrError] : nodeIdToRspOrError) {
                if (!rspOrError.IsOK()) {
                    // TODO(kvk1920): See the similar |TMapLikeNodeProxy::GetSelf| comment.
                    THROW_ERROR_EXCEPTION("Error getting requested information from master")
                        << rspOrError;
                }
                EmplaceOrCrash(
                    nodeIdToAttributes,
                    nodeId,
                    ConvertToAttributes(TYsonStringBuf(rspOrError.Value()->value())));
            }
        }

        writer.OnBeginList();
        for (const auto& child : children) {
            writer.OnListItem();
            if (attributeFilter) {
                const auto& attributes = GetOrCrash(nodeIdToAttributes, child.ChildId);
                ConsumeAttributes(&writer, attributes);
            }
            writer.OnStringScalar(child.ChildKey);
        }
        writer.OnEndList();

        auto result = WaitForFast(writer.Finish())
            .ValueOrThrow();
        response->set_value(ToProto(result));

        MaybeTouchCurrentNode(TYPathProxy::List, context);
        // Should not throw after this point.

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateNodeProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session,
    TSequoiaResolveResult resolveResult)
{
    auto type = TypeFromId(resolveResult.Id);
    ValidateSupportedSequoiaType(type);

    if (type == EObjectType::Document || type == EObjectType::Orchid) {
        return New<TOpaqueNodeProxy>(bootstrap, std::move(session), std::move(resolveResult));
    } else if (IsSequoiaCompositeNodeType(type)) {
        return New<TMapLikeNodeProxy>(bootstrap, std::move(session), std::move(resolveResult));
    } else {
        return New<TNodeProxy>(
            bootstrap,
            std::move(session),
            std::move(resolveResult));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
