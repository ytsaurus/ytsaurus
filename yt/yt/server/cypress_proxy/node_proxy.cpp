#include "node_proxy.h"

#include "private.h"
#include "action_helpers.h"
#include "bootstrap.h"
#include "helpers.h"
#include "path_resolver.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public TYPathServiceBase
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

    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, Get);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        DISPATCH_YPATH_SERVICE_METHOD(Set);
        DISPATCH_YPATH_SERVICE_METHOD(Remove);

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

    TCellTag GetRandomSequoiaNodeHostCellTag() const
    {
        auto connection = Bootstrap_->GetNativeConnection();

        YT_LOG_DEBUG("Started synchronizing master cell directory");
        const auto& cellDirectorySynchronizer = connection->GetMasterCellDirectorySynchronizer();
        WaitFor(cellDirectorySynchronizer->RecentSync())
            .ThrowOnError();
        YT_LOG_DEBUG("Master cell directory synchronized successfully");

        return connection->GetRandomMasterCellTagWithRoleOrThrow(
            NCellMasterClient::EMasterCellRole::SequoiaNodeHost);
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

        if (force) {
            // TODO(kvk1920): Current Cypress behaviour is just to ignore some errors.
            THROW_ERROR_EXCEPTION("Remove with \"force\" flag is not supported in Sequoia yet");
        }

        TCellTag subtreeRootCell;
        if (TypeFromId(Id_) == EObjectType::Scion) {
            subtreeRootCell = RemoveRootstock();
        } else {
            auto [parentPath, thisName] = DirNameAndBaseName(Path_);
            LockRowInPathToIdTable(parentPath, Transaction_);

            auto parentId = LookupNodeId(parentPath, Transaction_);
            DetachChild(parentId, thisName, Transaction_);
            subtreeRootCell = CellTagFromId(parentId);
        }

        auto mangledPath = MangleSequoiaPath(Path_);

        // NB: For non-recursive removal we have to check if directory is empty.
        // This can be done via requesting just 2 rows.
        auto selectedRowsLimit = recursive ? std::nullopt : std::optional(2);

        auto nodesToRemove = WaitFor(Transaction_->SelectRows<NRecords::TPathToNodeIdKey>(
            {
                Format("path >= %Qv", mangledPath),
                Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath)),
            },
            selectedRowsLimit))
            .ValueOrThrow();
        YT_VERIFY(nodesToRemove.size() >= 1);

        if (!recursive && nodesToRemove.size() > 1) {
            THROW_ERROR_EXCEPTION("Cannot remove non-empty composite node");
        }

        for (const auto& node : nodesToRemove) {
            RemoveNode(node.NodeId, node.Key.Path, Transaction_);
        }

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromCellTag(subtreeRootCell),
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        }))
            .ThrowOnError();

        context->Reply();
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

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
        limit,
        attributeFilter);

    auto newRequest = TYPathProxy::Get();
    newRequest->CopyFrom(*request);
    ForwardRequest(std::move(newRequest), response, context);
}

////////////////////////////////////////////////////////////////////////////////

class TMapLikeNodeProxy
    : public TNodeProxyBase
{
public:
    using TNodeProxyBase::TNodeProxyBase;

private:
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);
    DECLARE_YPATH_SERVICE_METHOD(NYTree::NProto, List);

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TNodeProxyBase::DoInvoke(context);
    }

    struct TRecursiveCreateResult
    {
        TString SubtreeRootKey;
        TNodeId SubtreeRootId;
        TNodeId DesignatedNodeId;
    };

    template <class TCallback>
    TRecursiveCreateResult CreateRecursive(
        const TYPath& parentPath,
        const std::vector<TYPathBuf>& childKeys,
        const TCallback& createDesignatedNode)
    {
        YT_VERIFY(!childKeys.empty());
        auto designatedPath = JoinNestedNodesToPath(parentPath, childKeys);
        auto designatedNodeId = createDesignatedNode(designatedPath);
        auto designatedNodeKey = TYPath(childKeys.back());
        auto prevNode = std::pair(designatedNodeId, designatedNodeKey);
        auto nestedPath = TYPathBuf(designatedPath).Chop(designatedNodeKey.size() + 1);
        for (auto it = std::next(childKeys.rbegin()); it != childKeys.rend(); ++it) {
            auto key = *it;
            auto id = Transaction_->GenerateObjectId(
                EObjectType::SequoiaMapNode,
                GetRandomSequoiaNodeHostCellTag());

            CreateNode(
                EObjectType::SequoiaMapNode,
                id,
                TYPath(nestedPath),
                Transaction_);
            AttachChild(id, prevNode.first, prevNode.second, Transaction_);

            nestedPath.Chop(key.size() + 1);
            prevNode = std::pair(id, key);
        }
        return TRecursiveCreateResult{
            .SubtreeRootKey = prevNode.second,
            .SubtreeRootId = prevNode.first,
            .DesignatedNodeId = designatedNodeId,
        };
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
    }

    class TTreeBuilder
        : public NYson::TForwardingYsonConsumer
    {
    public:
        TTreeBuilder(TMapLikeNodeProxy* owner)
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
        TMapLikeNodeProxy* Owner_;
        TString Key_;
        TYPath ParentPath_;
        TNodeId ResultNodeId_;
        std::stack<std::pair<TString, TNodeId>> NodeStack_;

        TNodeId CreateNode(EObjectType type) {
            auto cellTag = Owner_->GetRandomSequoiaNodeHostCellTag();
            auto nodeId = Owner_->Transaction_->GenerateObjectId(type, cellTag);
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
                /*removeRoot*/ false);
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
        context->SetRequestInfo();

        if (!request->force()) {
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

    void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* /*response*/,
        const TCtxSetPtr& context) override
    {
        // TODO(danilalexeev): Implement method _SetChild_ and bring out the common code with Create.
        context->SetRequestInfo();

        auto unresolvedSuffix = "/" + path;
        auto pathTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);

        if (!request->recursive() && std::ssize(pathTokens) > 1) {
            ThrowNoSuchChild(Path_, pathTokens[0]);
        }

        const auto& parentPath = Path_;
        auto parentId = Id_;

        // Acquire shared lock on parent node.
        LockRowInPathToIdTable(parentPath, Transaction_);

        auto createDesignatedNode = [&] (const TYPath& path) {
            TTreeBuilder builder(this);
            builder.BeginTree(path);
            auto producer = ConvertToProducer(NYson::TYsonString(request->value()));
            producer.Run(&builder);
            return builder.EndTree();
        };
        auto createResult = CreateRecursive(
            parentPath,
            pathTokens,
            createDesignatedNode);

        // Attach child on parent cell.
        AttachChild(parentId, createResult.SubtreeRootId, createResult.SubtreeRootKey, Transaction_);

        WaitFor(Transaction_->Commit({
            .CoordinatorCellId = CellIdFromObjectId(parentId),
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

        TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);

        // There is no composite node type other than Sequoia map node. If we
        // have unresolved suffix it can be either attribute or non-existent child.
        ThrowNoSuchChild(Path_, tokenizer.GetToken());
    }
};

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, Create)
{
    auto type = CheckedEnumCast<EObjectType>(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto recursive = request->recursive();
    auto force = request->force();
    auto ignoreTypeMismatch = request->ignore_type_mismatch();
    // TODO(h0pless): Decide what to do with hint id here.
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
    if (pathTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        WaitFor(Transaction_->Commit())
            .ThrowOnError();

        ToProto(response->mutable_node_id(), Id_);
        response->set_cell_tag(ToProto<int>(CellTagFromId(Id_)));
        context->Reply();
        return;
    }

    if (!recursive && std::ssize(pathTokens) > 1) {
        ThrowNoSuchChild(Path_, pathTokens[0]);
    }

    auto parentPath = Path_;
    auto parentId = Id_;

    // This should be in the if statement below, but because pathTokens works with TYPathBuf and DirNameAndBaseName returns TString
    // there is no other option but to create this weird holder.
    // TODO(h0pless): Fix this weirdness.
    auto [updatedParentPath, childKeyHolder] = DirNameAndBaseName(Path_);
    if (pathTokens.empty() && force) {
        parentPath = std::move(updatedParentPath);
        parentId = LookupNodeId(parentPath, Transaction_);
        pathTokens.push_back(childKeyHolder);

        auto removeFuture = RemoveSubtree(Path_, Transaction_);
        WaitFor(removeFuture)
            .ThrowOnError();
    }

    LockRowInPathToIdTable(parentPath, Transaction_);

    auto createDesignatedNode = [&] (const TYPath& path) {
        auto id = Transaction_->GenerateObjectId(type, GetRandomSequoiaNodeHostCellTag());
        CreateNode(type, id, path, Transaction_);
        return id;
    };
    auto createResult = CreateRecursive(
        parentPath,
        pathTokens,
        createDesignatedNode);

    // Attach child on parent cell.
    AttachChild(parentId, createResult.SubtreeRootId, createResult.SubtreeRootKey, Transaction_);

    WaitFor(Transaction_->Commit({
        .CoordinatorCellId = CellIdFromObjectId(parentId),
        .Force2PC = true,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();

    auto childCellTag = CellTagFromId(createResult.DesignatedNodeId);
    ToProto(response->mutable_node_id(), createResult.DesignatedNodeId);
    response->set_cell_tag(ToProto<int>(childCellTag));
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, List)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("Limit: %v, AttributeFilter: %v",
        limit,
        attributeFilter);

    auto unresolvedSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    if (auto pathTokens = TokenizeUnresolvedSuffix(unresolvedSuffix);
        !pathTokens.empty())
    {
        ThrowNoSuchChild(Path_, pathTokens[0]);
    }

    LockRowInPathToIdTable(Path_, Transaction_);
    auto selectRows = WaitFor(Transaction_->SelectRows<NRecords::TChildNodeKey>(
        {
            Format("parent_path = %Qv", MangleSequoiaPath(Path_)),
        },
        limit))
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

DEFINE_YPATH_SERVICE_METHOD(TMapLikeNodeProxy, Copy)
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

    // NB: Rewriting in case there were symlinks in the original source path.
    const auto& sourceRootPath = payload->ResolvedPrefix;
    if (!payload->UnresolvedSuffix.empty()) {
        auto unresolvedPathTokens = TokenizeUnresolvedSuffix(payload->UnresolvedSuffix);
        ThrowNoSuchChild(sourceRootPath, unresolvedPathTokens[0]);
    }

    // Validate there are no duplicate or missing destination nodes.
    auto unresolvedDestinationSuffix = GetRequestTargetYPath(context->GetRequestHeader());
    auto destinationPathTokens = TokenizeUnresolvedSuffix(unresolvedDestinationSuffix);
    if (destinationPathTokens.empty() && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(Path_);
        }

        // TODO(h0pless): If lockExisting - lock the node.
        WaitFor(Transaction_->Commit())
            .ThrowOnError();

        ToProto(response->mutable_node_id(), Id_);
        context->Reply();
        return;
    }

    if (!recursive && std::ssize(destinationPathTokens) > 1) {
        ThrowNoSuchChild(Path_, destinationPathTokens[0]);
    }

    auto nodesToCopy = SelectSubtree(sourceRootPath, Transaction_);
    auto destinationRootPath = Path_ + unresolvedDestinationSuffix;
    auto parentPath = Path_;
    auto parentId = Id_;

    auto overwriteDestinationSubtree = destinationPathTokens.empty() && force;
    // Same weirdness as the above. See Create.
    auto [updatedParentPath, childKey] = DirNameAndBaseName(Path_);
    if (overwriteDestinationSubtree) {
        parentPath = std::move(updatedParentPath);
        parentId = LookupNodeId(parentPath, Transaction_);
        DetachChild(parentId, childKey, Transaction_);
        destinationPathTokens.push_back(childKey);
    }

    if (options.Mode == ENodeCloneMode::Move) {
        auto [sourceParentPath, sourceRootKey] = DirNameAndBaseName(sourceRootPath);
        auto sourceParentId = LookupNodeId(sourceParentPath, Transaction_);
        DetachChild(sourceParentId, sourceRootKey, Transaction_);
    }

    std::vector<NRecords::TPathToNodeId> nodesToRemove;
    if (overwriteDestinationSubtree) {
        nodesToRemove = SelectSubtree(destinationRootPath, Transaction_);
        for (auto nodeToRemove : nodesToRemove) {
            // NB: Calling DelteRow and WriteRow under one transaction does not lead to conflict.
            // NB: It's fine to call RemoveNode early, since actions will be sorted in sequoia transaction.
            RemoveNode(nodeToRemove.NodeId, nodeToRemove.Key.Path, Transaction_);
        }
    }

    // Select returns sorted entries and destination subtree cannot include source subtree.
    // Thus to check that subtrees don't overlap it's enough to check source root with
    // first and last elements of the destination subtree.
    if (options.Mode == ENodeCloneMode::Move && (nodesToRemove.empty() ||
        sourceRootPath < DemangleSequoiaPath(nodesToRemove.front().Key.Path) ||
        DemangleSequoiaPath(nodesToRemove.back().Key.Path) < sourceRootPath))
    {
        for (auto nodesToCopy : nodesToCopy) {
            // NB: It's fine to call RemoveNode early, since actions will be sorted in sequoia transaction.
            RemoveNode(nodesToCopy.NodeId, nodesToCopy.Key.Path, Transaction_);
        }
    }

    LockRowInPathToIdTable(parentPath, Transaction_);

    auto destinationId = CopySubtree(nodesToCopy, sourceRootPath, destinationRootPath, options, Transaction_);

    // Designated node is cloned source root node. It has already been created and it's ID is known, yet it hasn't been attached.
    auto getDesignatedNodeId = [&] (const TYPath& path) {
        YT_VERIFY(path == destinationRootPath);
        return destinationId;
    };
    auto createResult = CreateRecursive(
        parentPath,
        destinationPathTokens,
        getDesignatedNodeId);
    AttachChild(parentId, createResult.SubtreeRootId, createResult.SubtreeRootKey, Transaction_);

    WaitFor(Transaction_->Commit({
        .CoordinatorCellId = CellIdFromObjectId(parentId),
        .Force2PC = true,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    }))
        .ThrowOnError();

    ToProto(response->mutable_node_id(), destinationId);
    context->Reply();
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
    } else {
        proxy = New<TNodeProxyBase>(bootstrap, id, std::move(resolvedPath), std::move(transaction));
    }
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
