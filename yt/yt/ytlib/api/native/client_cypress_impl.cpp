#include "client_impl.h"

#include "config.h"
#include "connection.h"
#include "transaction.h"
#include "type_handler.h"

// COMPAT(kvk1920)
#include <yt/yt/ytlib/api/native/proto/transaction_actions.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/yt/ytlib/chunk_client/fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schema_inferer.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/generic/algorithm.h>

namespace NYT::NApi::NNative {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TRequestPtr>
void SetCloneNodeBaseRequestParameters(
    const TRequestPtr& req,
    const TCopyNodeOptionsBase& options)
{
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_creation_time(options.PreserveCreationTime);
    req->set_preserve_modification_time(options.PreserveModificationTime);
    req->set_enable_cross_cell_copying(options.EnableCrossCellCopying);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);
    req->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
    req->set_preserve_owner(options.PreserveOwner);
    req->set_preserve_acl(options.PreserveAcl);
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
}

template <class TRequestPtr>
void SetCopyNodeBaseRequestParameters(
    const TRequestPtr& req,
    const TCopyNodeOptions& options)
{
    SetCloneNodeBaseRequestParameters(req, options);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
}

template <class TRequestPtr>
void SetMoveNodeBaseRequestParameters(
    const TRequestPtr& req,
    const TMoveNodeOptions& options)
{
    SetCloneNodeBaseRequestParameters(req, options);
}

void SetCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqCopyPtr& req,
    const TCopyNodeOptions& options)
{
    SetCopyNodeBaseRequestParameters(req, options);
    req->set_mode(static_cast<int>(ENodeCloneMode::Copy));
}

void SetCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_mode(static_cast<int>(ENodeCloneMode::Move));
}

void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TCopyNodeOptions& /*options*/)
{
    req->set_mode(static_cast<int>(ENodeCloneMode::Copy));
}

void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TMoveNodeOptions& /*options*/)
{
    req->set_mode(static_cast<int>(ENodeCloneMode::Move));
}

void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TCopyNodeOptions& options)
{
    SetCopyNodeBaseRequestParameters(req, options);
    req->set_mode(static_cast<int>(ENodeCloneMode::Copy));
}

void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_mode(static_cast<int>(ENodeCloneMode::Move));
}

class TCrossCellExecutor
{
protected:
    TCrossCellExecutor(
        TClientPtr client,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Logger(std::move(logger))
    { }

    ~TCrossCellExecutor()
    {
        try {
            MaybeAbortTransaction();
        } catch (std::exception& e) {
            YT_LOG_DEBUG(TError(e), "Error aborting transaction");
        }
    }

    struct TSerializedSubtree
    {
        TSerializedSubtree() = default;

        TSerializedSubtree(
            TYPath path,
            NCypressClient::NProto::TSerializedTree serializedValue,
            NProtoBuf::RepeatedPtrField<NCypressClient::NProto::TRegisteredSchema> schemas)
            : Path(std::move(path))
            , SerializedValue(std::move(serializedValue))
            , Schemas(std::move(schemas))
        { }

        //! Relative to tree root path to subtree root.
        TYPath Path;

        //! Serialized subtree.
        NCypressClient::NProto::TSerializedTree SerializedValue;

        //! These schemas are referenced from #SerializedValue (by serialization keys).
        NProtoBuf::RepeatedPtrField<NCypressClient::NProto::TRegisteredSchema> Schemas;
    };

    const TClientPtr Client_;

    const NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    bool TransactionCommitted_ = false;

    std::vector<TSerializedSubtree> SerializedSubtrees_;

    TNodeId SrcNodeId_;
    TYPath ResolvedSrcNodePath_;
    TNodeId DstNodeId_;

    std::vector<TCellTag> ExternalCellTags_;


    template <class TOptions>
    void StartTransaction(
        const TString& title,
        const TOptions& options)
    {
        YT_LOG_DEBUG("Starting transaction");

        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", title);

        TTransactionStartOptions transactionOptions;
        transactionOptions.ParentId = options.TransactionId;
        transactionOptions.Attributes = std::move(transactionAttributes);
        transactionOptions.StartCypressTransaction = true;
        auto transactionOrError = WaitFor(Client_->StartNativeTransaction(
            ETransactionType::Master,
            transactionOptions));
        THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting transaction");
        Transaction_ = transactionOrError.Value();

        YT_LOG_DEBUG("Transaction started (TransactionId: %v)",
            Transaction_->GetId());
    }

    template <class TOptions>
    void BeginCopy(const TYPath& srcPath, const TOptions& options, bool allowRootLink)
    {
        if (allowRootLink) {
            ResolveSourceNode(srcPath);
        } else {
            ResolvedSrcNodePath_ = srcPath;
        }

        auto proxy = CreateObjectServiceWriteProxy(Client_);

        std::queue<TYPath> subtreeSerializationQueue;
        subtreeSerializationQueue.push(ResolvedSrcNodePath_);

        while (!subtreeSerializationQueue.empty()) {
            auto subtreePath = subtreeSerializationQueue.front();
            subtreeSerializationQueue.pop();

            YT_LOG_DEBUG("Requesting serialized subtree (SubtreePath: %v)", subtreePath);

            auto batchReq = proxy.ExecuteBatch();
            auto req = TCypressYPathProxy::BeginCopy(subtreePath);
            GenerateMutationId(req);
            SetTransactionId(req, Transaction_->GetId());
            SetBeginCopyNodeRequestParameters(req, options);
            batchReq->AddRequest(req);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
                "Error requesting serialized subtree for %v", subtreePath);

            const auto& batchRsp = batchRspOrError.Value();

            auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspBeginCopy>(0);
            const auto& rsp = rspOrError.Value();
            auto portalChildIds = FromProto<std::vector<TNodeId>>(rsp->portal_child_ids());
            auto externalCellTags = FromProto<TCellTagList>(rsp->external_cell_tags());
            auto opaqueChildPaths = FromProto<std::vector<TYPath>>(rsp->opaque_child_paths());
            auto nodeId = FromProto<TNodeId>(rsp->node_id());

            if (!allowRootLink && subtreePath == srcPath) {
                SrcNodeId_ = nodeId;
            }

            YT_LOG_DEBUG("Serialized subtree received (NodeId: %v, Path: %v, FormatVersion: %v, TreeSize: %v, "
                "PortalChildIds: %v, ExternalCellTags: %v, OpaqueChildPaths: %v, RegisteredSchemaCount: %v)",
                nodeId,
                subtreePath,
                rsp->serialized_tree().version(),
                rsp->serialized_tree().data().size(),
                portalChildIds,
                externalCellTags,
                opaqueChildPaths,
                rsp->schemas_size());

            auto relativePath = TryComputeYPathSuffix(subtreePath, ResolvedSrcNodePath_);
            YT_VERIFY(relativePath);

            SerializedSubtrees_.emplace_back(
                *relativePath,
                std::move(*rsp->mutable_serialized_tree()),
                std::move(*rsp->mutable_schemas()));

            for (auto cellTag : externalCellTags) {
                ExternalCellTags_.push_back(cellTag);
            }

            for (const auto& opaqueChildPath : opaqueChildPaths) {
                subtreeSerializationQueue.push(opaqueChildPath);
            }
        }

        SortUnique(ExternalCellTags_);
    }

    void ResolveSourceNode(const TYPath& srcPath)
    {
        auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower);

        YT_LOG_DEBUG("Resolving source node (SourceNodePath: %v)", srcPath);

        auto batchReq = proxy.ExecuteBatch();
        auto req = TYPathProxy::Get(srcPath + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{"id", "path"});
        SetTransactionId(req, Transaction_->GetId());
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Error resolving source node %v", srcPath);

        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspGet>(0);
        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
        SrcNodeId_ = attributes->Get<TNodeId>("id");
        ResolvedSrcNodePath_ = attributes->Get<TYPath>("path");

        YT_LOG_DEBUG("Source node resolved successfully (SourceNodePath: %v, SourceNodeId: %v, ResolvedSrcNodePath: %v)",
            srcPath,
            SrcNodeId_,
            ResolvedSrcNodePath_);
    }

    template <class TOptions>
    void EndCopy(const TYPath& dstPath, const TOptions& options, bool inplace)
    {
        YT_LOG_DEBUG("Materializing serialized subtrees");

        auto proxy = CreateObjectServiceWriteProxy(Client_);

        for (auto& subtree : SerializedSubtrees_) {
            auto batchReq = proxy.ExecuteBatch();
            auto req = TCypressYPathProxy::EndCopy(dstPath + subtree.Path);
            GenerateMutationId(req);
            SetTransactionId(req, Transaction_->GetId());
            SetEndCopyNodeRequestParameters(req, options);
            req->set_inplace(inplace);
            *req->mutable_serialized_tree() = std::move(subtree.SerializedValue);
            *req->mutable_schemas() = std::move(subtree.Schemas);
            batchReq->AddRequest(req);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
                "Error materializing serialized subtree");

            const auto& batchRsp = batchRspOrError.Value();
            for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspEndCopy>()) {
                const auto& rsp = rspOrError.ValueOrThrow();
                if (subtree.Path == TYPath()) {
                    DstNodeId_ = FromProto<TNodeId>(rsp->node_id());
                }
            }

            inplace = true;
        }

        YT_LOG_DEBUG("Serialized subtrees materialized (RootNodeId: %v)",
            DstNodeId_);
    }

    void SyncExternalCellsWithSourceNodeCell()
    {
        SyncExternalCellsWithNodeCell(SrcNodeId_);
    }

    void SyncExternalCellsWithClonedNodeCell()
    {
        SyncExternalCellsWithNodeCell(DstNodeId_);
    }

    void SyncExternalCellsWithNodeCell(TNodeId nodeId)
    {
        if (ExternalCellTags_.empty()) {
            return;
        }

        YT_VERIFY(nodeId == SrcNodeId_ || nodeId == DstNodeId_);
        auto nodeKind = nodeId == SrcNodeId_ ? "source" : "cloned";

        YT_LOG_DEBUG("Synchronizing external cells with the %v node cell",
            nodeKind);

        auto nodeCellTag = CellTagFromId(nodeId);
        const auto& connection = Client_->GetNativeConnection();
        std::vector<TFuture<void>> futures;
        for (auto externalCellTag : ExternalCellTags_) {
            futures.push_back(connection->SyncHiveCellWithOthers(
                {connection->GetMasterCellId(nodeCellTag)},
                connection->GetMasterCellId(externalCellTag)));
        }

        auto error = WaitFor(AllSucceeded(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing external cells with the %v node cell",
            nodeKind);

        YT_LOG_DEBUG("External cells synchronized with the %v node cell",
            nodeKind);
    }

    void CommitTransaction(const TTransactionCommitOptions& options = {})
    {
        YT_LOG_DEBUG("Committing transaction");

        // NB: failing to commit still means we shouldn't try to abort.
        TransactionCommitted_ = true;
        auto error = WaitFor(Transaction_->Commit(options));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error committing transaction");

        YT_LOG_DEBUG("Transaction committed");
    }

    void MaybeAbortTransaction()
    {
        if (!Transaction_ || TransactionCommitted_) {
            return;
        }

        YT_LOG_DEBUG("Aborting transaction");

        auto error = WaitFor(Transaction_->Abort());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error aborting transaction");

        YT_LOG_DEBUG("Transaction aborted");
    }
};

template <class TOptions>
class TCrossCellNodeCloner
    : public TCrossCellExecutor
{
public:
    TCrossCellNodeCloner(
        TClientPtr client,
        TYPath srcPath,
        TYPath dstPath,
        const TOptions& options,
        const NLogging::TLogger& logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.WithTag("SrcPath: %v, DstPath: %v",
                srcPath,
                dstPath))
        , SrcPath_(std::move(srcPath))
        , DstPath_(std::move(dstPath))
        , Options_(options)
    { }

    TNodeId Run()
    {
        YT_LOG_DEBUG("Cross-cell node cloning started");
        StartTransaction(
            Format("Clone %v to %v", SrcPath_, DstPath_),
            Options_);
        BeginCopy(SrcPath_, Options_, true);
        SyncExternalCellsWithSourceNodeCell();
        EndCopy(DstPath_, Options_, false);
        if constexpr(std::is_assignable_v<TOptions, TMoveNodeOptions>) {
            RemoveSource();
        }
        SyncExternalCellsWithClonedNodeCell();
        TTransactionCommitOptions commitOptions = {};
        commitOptions.PrerequisiteTransactionIds = Options_.PrerequisiteTransactionIds;
        CommitTransaction(commitOptions);
        YT_LOG_DEBUG("Cross-cell node cloning completed");
        return DstNodeId_;
    }

private:
    const TYPath SrcPath_;
    const TYPath DstPath_;
    const TOptions Options_;


    void RemoveSource()
    {
        YT_LOG_DEBUG("Removing source node");

        auto error = WaitFor(Transaction_->RemoveNode(FromObjectId(SrcNodeId_)));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error removing source node");

        YT_LOG_DEBUG("Source node removed");
    }
};

class TNodeExternalizer
    : public TCrossCellExecutor
{
public:
    TNodeExternalizer(
        TClientPtr client,
        TYPath path,
        TCellTag cellTag,
        const TExternalizeNodeOptions& options,
        const NLogging::TLogger& logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.WithTag("Path: %v, CellTag: %v",
                path,
                cellTag))
        , Path_(std::move(path))
        , CellTag_(cellTag)
        , Options_(options)
    { }

    void Run()
    {
        YT_LOG_DEBUG("Node externalization started");
        StartTransaction(
            Format("Externalize %v to %v", Path_, CellTag_),
            Options_);
        RequestAclAndAnnotation();
        BeginCopy(Path_, GetOptions(), false);
        SyncExternalCellsWithSourceNodeCell();
        if (TypeFromId(SrcNodeId_) != EObjectType::MapNode) {
            THROW_ERROR_EXCEPTION("%v is not a map node", Path_);
        }
        CreatePortal();
        SyncExitCellWithEntranceCell();
        EndCopy(Path_, GetOptions(), /*inplace*/ true);
        SyncExternalCellsWithClonedNodeCell();
        CommitTransaction();
        YT_LOG_DEBUG("Node externalization completed");
    }

private:
    const TYPath Path_;
    const TCellTag CellTag_;
    const TExternalizeNodeOptions Options_;
    TYsonString Acl_;
    TYsonString InheritAcl_;
    TYsonString Annotation_;

    static TMoveNodeOptions GetOptions()
    {
        TMoveNodeOptions options;
        options.PreserveAccount = true;
        options.PreserveCreationTime = true;
        options.PreserveModificationTime = true;
        options.PreserveExpirationTime = true;
        options.PreserveOwner = true;
        options.Force = true;
        return options;
    }

    void RequestAclAndAnnotation()
    {
        YT_LOG_DEBUG("Requesting root @acl, @inherit_acl and @annotation");

        auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower);

        auto batchReq = proxy.ExecuteBatch();

        auto getAttribute = [&] (TString name, TYsonString* result) {
            auto req = TObjectYPathProxy::Get(Path_ + "/@" + name);
            req->Tag() = result;
            batchReq->AddRequest(req);
        };

        getAttribute("acl", &Acl_);
        getAttribute("inherit_acl", &InheritAcl_);
        getAttribute("annotation", &Annotation_);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of portal exit node");
        const auto& batchRsp = batchRspOrError.Value();

        for (auto rspOrError : batchRsp->GetResponses<TObjectYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            *std::any_cast<TYsonString*>(rsp->Tag()) = TYsonString(rsp->value());
        }

        YT_LOG_DEBUG("Root @acl, @inherit_acl and @annotation received");
    }

    void CreatePortal()
    {
        YT_LOG_DEBUG("Creating portal");

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("exit_cell_tag", CellTag_);
        attributes->Set("inherit_acl", InheritAcl_);
        attributes->Set("annotation", Annotation_);
        attributes->Set("acl", Acl_);

        TCreateNodeOptions options;
        options.Attributes = std::move(attributes);
        options.Force = true;

        auto nodeIdOrError = WaitFor(Transaction_->CreateNode(
            Path_,
            EObjectType::PortalEntrance,
            options));
        THROW_ERROR_EXCEPTION_IF_FAILED(nodeIdOrError, "Error creating portal");

        YT_LOG_DEBUG("Portal created");
    }

    void SyncExitCellWithEntranceCell()
    {
        YT_LOG_DEBUG("Synchronizing exit cell with entrance cell");

        const auto& connection = Client_->GetNativeConnection();
        auto future = connection->SyncHiveCellWithOthers(
            {connection->GetMasterCellId(CellTagFromId(SrcNodeId_))},
            connection->GetMasterCellId(CellTag_));

        auto error = WaitFor(future);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing exit cell with entrance cell");

        YT_LOG_DEBUG("Exit cell synchronized with entrance cell");
    }
};

class TNodeInternalizer
    : public TCrossCellExecutor
{
public:
    TNodeInternalizer(
        TClientPtr client,
        TYPath path,
        const TInternalizeNodeOptions& options,
        const NLogging::TLogger& logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.WithTag("Path: %v", path))
        , Path_(std::move(path))
        , Options_(options)
    { }

    void Run()
    {
        YT_LOG_DEBUG("Node internalization started");

        StartTransaction(
            Format("Internalize %v", Path_),
            Options_);

        BeginCopy(Path_, GetOptions(), false);

        SyncExternalCellsWithSourceNodeCell();
        if (TypeFromId(SrcNodeId_) != EObjectType::PortalExit) {
            THROW_ERROR_EXCEPTION("%v is not a portal", Path_);
        }

        auto clonedNodeId = CreateMapNode();

        EndCopy(Path_ + "&", GetOptions(), true);

        SyncExternalCellsWithClonedNodeCell();

        // NB: This can be racy, but the situation when user sets acl while the node is being internalized is rare enough
        // so we don't want to overcomplicate the code here.
        auto aclOrError = WaitFor(Client_->GetNode(FromObjectId(SrcNodeId_) + "/@acl", {}));
        THROW_ERROR_EXCEPTION_IF_FAILED(aclOrError, "Error getting root effective ACL");

        CommitTransaction();

        auto rootEffectiveAcl =  aclOrError.Value();
        WaitFor(Client_->SetNode(FromObjectId(clonedNodeId) + "/@acl", rootEffectiveAcl, {}))
            .ThrowOnError();

        YT_LOG_DEBUG("Node internalization completed");
    }

private:
    const TYPath Path_;
    const TInternalizeNodeOptions Options_;

    TNodeId GetEntranceNodeId()
    {
        auto nodeIdOrError = WaitFor(Transaction_->GetNode(Path_ + "/@entrance_node_id"));
        auto nodeId = ConvertTo<TNodeId>(nodeIdOrError.ValueOrThrow());
        return nodeId;
    }

    static TMoveNodeOptions GetOptions()
    {
        TMoveNodeOptions options;
        options.PreserveAccount = true;
        options.PreserveCreationTime = true;
        options.PreserveModificationTime = true;
        options.PreserveExpirationTime = true;
        options.PreserveOwner = false;
        options.Force = true;
        options.PreserveAcl = false;
        return options;
    }

    TNodeId CreateMapNode()
    {
        YT_LOG_DEBUG("Creating map node");

        TCreateNodeOptions options;
        options.Force = true;

        auto nodeIdOrError = WaitFor(Transaction_->CreateNode(
            Path_ + "&",
            EObjectType::MapNode,
            options));
        THROW_ERROR_EXCEPTION_IF_FAILED(nodeIdOrError, "Error creating map node");

        YT_LOG_DEBUG("Map node created (NodeId: %v)", nodeIdOrError.Value());

        return nodeIdOrError.Value();
    }
};

} // namespace

TNodeId TClient::CreateNodeImpl(
    EObjectType type,
    const TYPath& path,
    const IAttributeDictionary& attributes,
    const TCreateNodeOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Create(path);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    req->set_type(static_cast<int>(type));
    req->set_recursive(options.Recursive);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
    req->set_force(options.Force);
    req->set_ignore_type_mismatch(options.IgnoreTypeMismatch);
    ToProto(req->mutable_node_attributes(), attributes);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0)
        .ValueOrThrow();
    return FromProto<TNodeId>(rsp->node_id());
}

TYsonString TClient::DoGetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->GetNode(path, options)) {
            return *result;
        }
    }
    YT_ABORT();
}

void TClient::DoSetNode(
    const TYPath& path,
    const TYsonString& value,
    const TSetNodeOptions& options)
{
    MaybeValidateExternalObjectPermission(path, EPermission::Write);

    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TYPathProxy::Set(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetMutationId(req, options);

    auto nestingLevelLimit = Connection_->GetConfig()->CypressWriteYsonNestingLevelLimit;
    YT_VERIFY(value.GetType() == EYsonType::Node);
    auto binarizedValue = ConvertToYsonStringNestingLimited(value, nestingLevelLimit);

    req->set_value(binarizedValue.ToString());

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TYPathProxy::TRspSet>(0)
        .ThrowOnError();
}

void TClient::DoMultisetAttributesNode(
    const TYPath& path,
    const IMapNodePtr& attributes,
    const TMultisetAttributesNodeOptions& options)
{
    MaybeValidateExternalObjectPermission(path, EPermission::Write);

    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TYPathProxy::MultisetAttributes(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetMutationId(req, options);

    req->set_force(options.Force);

    auto children = attributes->GetChildren();
    std::sort(children.begin(), children.end());
    auto nestingLevelLimit = Connection_->GetConfig()->CypressWriteYsonNestingLevelLimit;
    for (const auto& [attribute, value] : children) {
        auto* protoSubrequest = req->add_subrequests();
        protoSubrequest->set_attribute(attribute);
        auto binarizedValue = ConvertToYsonStringNestingLimited(value, nestingLevelLimit);
        protoSubrequest->set_value(binarizedValue.ToString());
    }

    batchReq->AddRequest(req);
    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TYPathProxy::TRspMultisetAttributes>(0)
        .ThrowOnError();
}

void TClient::DoRemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->RemoveNode(path, options)) {
            return;
        }
    }
    YT_ABORT();
}

TYsonString TClient::DoListNode(
    const TYPath& path,
    const TListNodeOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->ListNode(path, options)) {
            return *result;
        }
    }
    YT_ABORT();
}

TNodeId TClient::DoCreateNode(
    const TYPath& path,
    EObjectType type,
    const TCreateNodeOptions& options)
{
    auto newOptions = options;
    if (newOptions.Attributes) {
        const auto nestingLevelLimit = Connection_->GetConfig()->CypressWriteYsonNestingLevelLimit;
        auto newAttributes = CreateEphemeralAttributes();
        for (const auto& [key, value] : options.Attributes->ListPairs()) {
            auto binarizedValue = ConvertToYsonStringNestingLimited(value, nestingLevelLimit);
            newAttributes->SetYson(key, binarizedValue);
        }
        newOptions.Attributes = std::move(newAttributes);
    }

    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->CreateNode(type, path, newOptions)) {
            return *result;
        }
    }
    YT_ABORT();
}

TLockNodeResult TClient::DoLockNode(
    const TYPath& path,
    ELockMode mode,
    const TLockNodeOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();

    auto batchReqConfig = New<TReqExecuteBatchWithRetriesConfig>();

    auto batchReq = proxy.ExecuteBatchWithRetries(std::move(batchReqConfig));
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Lock(path);
    SetTransactionId(req, options, false);
    SetMutationId(req, options);
    req->set_mode(static_cast<int>(mode));
    req->set_waitable(options.Waitable);
    if (options.ChildKey) {
        req->set_child_key(*options.ChildKey);
    }
    if (options.AttributeKey) {
        req->set_attribute_key(*options.AttributeKey);
    }
    auto timestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();
    req->set_timestamp(timestamp);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspLock>(0)
        .ValueOrThrow();

    return TLockNodeResult{
        FromProto<TLockId>(rsp->lock_id()),
        FromProto<TNodeId>(rsp->node_id()),
        rsp->revision()
    };
}

void TClient::DoUnlockNode(
    const TYPath& path,
    const TUnlockNodeOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();

    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Unlock(path);
    SetTransactionId(req, options, false);
    SetMutationId(req, options);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspUnlock>(0)
        .ValueOrThrow();
}

TNodeId TClient::DoCopyNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    return DoCloneNode(srcPath, dstPath, options);
}

TNodeId TClient::DoMoveNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    return DoCloneNode(srcPath, dstPath, options);
}

template <class TOptions>
TNodeId TClient::DoCloneNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();

    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Copy(dstPath);
    SetCopyNodeRequestParameters(req, options);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    auto* ypathExt = req->Header().MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    ypathExt->add_additional_paths(srcPath);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspCopy>(0);

    if (rspOrError.GetCode() != NObjectClient::EErrorCode::CrossCellAdditionalPath) {
        auto rsp = rspOrError.ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    if (!options.EnableCrossCellCopying) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::CrossCellAdditionalPath,
            "Cross-cell \"copy\"/\"move\" command is explicitly disabled by request options");
    }

    if (!options.PrerequisiteRevisions.empty()) {
        THROW_ERROR_EXCEPTION("Cross-cell \"copy\"/\"move\" command does not support prerequisite revisions");
    }

    if (options.Retry) {
        THROW_ERROR_EXCEPTION("Cross-cell \"copy\"/\"move\" command is not retriable");
    }

    TCrossCellNodeCloner<TOptions> cloner(
        this,
        srcPath,
        dstPath,
        options,
        Logger);
    return cloner.Run();
}

TNodeId TClient::DoLinkNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();

    if (!options.Force) {
        auto batchReq = proxy.ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Exists(srcPath);
        SetTransactionId(req, options, true);
        batchReq->AddRequest(req);
        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspExists>(0)
            .ValueOrThrow();
        if (!rsp->value()) {
            THROW_ERROR_EXCEPTION("Target %v for the link %v does not exist.",
                srcPath,
                dstPath);
        }
    }

    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Create(dstPath);
    req->set_type(static_cast<int>(EObjectType::Link));
    req->set_recursive(options.Recursive);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
    req->set_force(options.Force);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    auto attributes = options.Attributes ? ConvertToAttributes(options.Attributes.Get()) : CreateEphemeralAttributes();
    attributes->Set("target_path", srcPath);
    ToProto(req->mutable_node_attributes(), *attributes);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0)
        .ValueOrThrow();
    return FromProto<TNodeId>(rsp->node_id());
}

class TNodeConcatenator
{
public:
    TNodeConcatenator(
        TClientPtr client,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Logger(std::move(logger))
    { }

    void ConcatenateNodes(
        const std::vector<TRichYPath>& srcPaths,
        const TRichYPath& dstPath,
        TConcatenateNodesOptions options)
    {
        Options_ = std::move(options);
        TransactionId_ = Client_->GetTransactionId(options, /*allowNullTransaction=*/true);
        Append_ = dstPath.GetAppend();

        try {
            CreateObjects(srcPaths, dstPath);
            StartNestedInputTransactions();
            GetObjectAttributes();
            GetCommonType();
            GetSrcObjectChunkCounts();
            if (CommonType_ == EObjectType::Table) {
                InferOutputTableSchema();
            }
            FetchChunkSpecs();
            if (Sorted_) {
                ValidateChunkSchemas();
                OrderChunks();
                ValidateChunkRanges();
                if (Append_) {
                    ValidateBoundaryKeys();
                }
            }
            BeginUpload();
            TeleportChunks();
            GetUploadParams();
            UploadChunks();
            EndUpload();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error concatenating %v to %v",
                srcPaths,
                dstPath)
                << ex;
        }
    }

private:
    const TClientPtr Client_;
    const NLogging::TLogger Logger;
    TConcatenateNodesOptions Options_;

    std::vector<TUserObject> SrcObjects_;
    TUserObject DstObject_;

    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs_;

    TTransactionId TransactionId_;
    std::vector<NApi::ITransactionPtr> NestedInputTransactions_;

    bool Append_ = false;
    bool Sorted_ = false;

    EObjectType CommonType_;

    std::vector<TTableSchema> InputTableSchemas_;
    TTableSchemaPtr OutputTableSchema_;
    ETableSchemaMode OutputTableSchemaMode_;

    TTransactionPtr UploadTransaction_;

    TChunkListId ChunkListId_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    void CreateObjects(
        const std::vector<TRichYPath>& srcPaths,
        TRichYPath dstPath)
    {
        auto createObject = [&] (const TRichYPath& path) {
            auto transactionId = path.GetTransactionId().value_or(TransactionId_);
            return TUserObject(path, transactionId);
        };

        SrcObjects_.reserve(srcPaths.size());
        for (const auto& srcPath : srcPaths) {
            SrcObjects_.push_back(createObject(srcPath));
        }

        DstObject_ = createObject(dstPath);
    }

    void StartNestedInputTransactions()
    {
        THashMap<TTransactionId, int> transactionIdToIndex;
        for (const auto& srcObject : SrcObjects_) {
            auto transactionId = srcObject.TransactionId;
            YT_VERIFY(transactionId);
            if (transactionIdToIndex.find(*transactionId) == transactionIdToIndex.end()) {
                auto transactionIndex = transactionIdToIndex.size();
                YT_VERIFY(transactionIdToIndex.emplace(*transactionId, transactionIndex).second);
            }
        }

        std::vector<TFuture<NApi::ITransactionPtr>> asyncTransactions;
        asyncTransactions.resize(transactionIdToIndex.size());
        for (auto [transactionId, index] : transactionIdToIndex) {
            TTransactionStartOptions options;
            options.Ping = true;
            options.PingAncestors = false;
            options.Timeout = Client_->Connection_->GetConfig()->NestedInputTransactionTimeout;
            options.PingPeriod = Client_->Connection_->GetConfig()->NestedInputTransactionPingPeriod;
            options.ParentId = transactionId;

            asyncTransactions[index] = Client_->StartTransaction(ETransactionType::Master, options);

            YT_LOG_DEBUG("Starting nested input transaction (ParentTransactionId: %v, TransactionIndex: %v)",
                transactionId,
                index);
        }

        auto transactionsOrError = WaitFor(AllSucceeded(asyncTransactions));
        THROW_ERROR_EXCEPTION_IF_FAILED(transactionsOrError, "Failed to start nested input transactions");
        NestedInputTransactions_ = transactionsOrError.Value();
        YT_VERIFY(NestedInputTransactions_.size() == transactionIdToIndex.size());

        for (auto& srcObject : SrcObjects_) {
            auto transactionId = *srcObject.TransactionId;
            auto transactionIndex = GetOrCrash(transactionIdToIndex, transactionId);
            auto nestedTransactionId = NestedInputTransactions_[transactionIndex]->GetId();
            YT_LOG_DEBUG("Setting nested input transaction id for source object (ObjectPath: %v, NestedTransactionId: %v)",
                srcObject.GetPath(),
                nestedTransactionId);
            srcObject.TransactionId = nestedTransactionId;
        }
    }

    void GetObjectAttributes()
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());
        auto batchReq = proxy.ExecuteBatch();

        for (auto& srcObject : SrcObjects_) {
            auto req = TObjectYPathProxy::GetBasicAttributes(srcObject.GetPath());
            req->set_populate_security_tags(true);
            req->Tag() = &srcObject;
            NCypressClient::SetTransactionId(req, *srcObject.TransactionId);
            batchReq->AddRequest(req, "get_src_attributes");
        }

        {
            auto req = TObjectYPathProxy::GetBasicAttributes(DstObject_.GetPath());
            req->Tag() = &DstObject_;
            NCypressClient::SetTransactionId(req, *DstObject_.TransactionId);
            batchReq->AddRequest(req, "get_dst_attributes");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting basic attributes of inputs and outputs");
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_src_attributes");
            for (const auto& rspOrError : rspsOrError) {
                const auto& rsp = rspOrError.Value();
                auto* srcObject = std::any_cast<TUserObject*>(rsp->Tag());

                srcObject->ObjectId = FromProto<TObjectId>(rsp->object_id());
                srcObject->ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
                srcObject->ExternalTransactionId = rsp->has_external_transaction_id()
                    ? FromProto<TTransactionId>(rsp->external_transaction_id())
                    : *srcObject->TransactionId;
                srcObject->SecurityTags = FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());

                YT_LOG_DEBUG("Source object attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v, SecurityTags: %v, ExternalTransactionId: %v)",
                    srcObject->GetPath(),
                    srcObject->ObjectId,
                    srcObject->ExternalCellTag,
                    srcObject->SecurityTags,
                    srcObject->ExternalTransactionId);
            }
        }
        {
            auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_dst_attributes");
            YT_VERIFY(rspsOrError.size() == 1);
            const auto& rsp = rspsOrError[0].Value();
            auto* dstObject = std::any_cast<TUserObject*>(rsp->Tag());

            dstObject->ObjectId = FromProto<TObjectId>(rsp->object_id());
            dstObject->ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());

            YT_LOG_DEBUG("Destination object attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v)",
                dstObject->GetPath(),
                dstObject->ObjectId,
                dstObject->GetObjectIdPath());
        }
    }

    void GetCommonType()
    {
        auto isValidType = [&] (EObjectType type) {
            return type == EObjectType::Table || type == EObjectType::File;
        };

        DstObject_.Type = TypeFromId(DstObject_.ObjectId);

        // For virtual tables object types cannot be inferred from object ids.
        bool needToFetchSourceObjectTypes = false;
        for (auto& srcObject : SrcObjects_) {
            srcObject.Type = TypeFromId(srcObject.ObjectId);
            if (!isValidType(srcObject.Type)) {
                needToFetchSourceObjectTypes = true;
            }
        }

        if (needToFetchSourceObjectTypes) {
            FetchSourceObjectTypes();
        }

        std::optional<EObjectType> commonType;
        TString pathWithCommonType;

        auto checkType = [&] (const TUserObject& object) {
            auto type = object.Type;
            if (!isValidType(type)) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv or %Qlv, %Qlv found",
                    object.GetPath(),
                    EObjectType::Table,
                    EObjectType::File,
                    type);
            }
            if (commonType && *commonType != type) {
                THROW_ERROR_EXCEPTION("Type of %v (%Qlv) must be the same as type of %v (%Qlv)",
                    object.GetPath(),
                    type,
                    pathWithCommonType,
                    *commonType);
            }
            commonType = type;
            pathWithCommonType = object.GetPath();
        };

        for (const auto& srcObject : SrcObjects_) {
            checkType(srcObject);
        }
        checkType(DstObject_);

        CommonType_ = *commonType;
    }

    void FetchSourceObjectTypes()
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());
        auto batchReq = proxy.ExecuteBatch();

        for (auto& srcObject : SrcObjects_) {
            auto req = TObjectYPathProxy::Get(srcObject.GetPath() + "/@");
            req->Tag() = &srcObject;
            req->mutable_attributes()->add_keys("type");
            NCypressClient::SetTransactionId(req, *srcObject.TransactionId);
            batchReq->AddRequest(req, "get_src_object_types");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching source object types");
        const auto& batchRsp = batchRspOrError.Value();
        auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGet>("get_src_object_types");
        for (const auto& rspOrError : rspsOrError) {
            const auto& rsp = rspOrError.Value();
            auto* srcObject = std::any_cast<TUserObject*>(rsp->Tag());
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            srcObject->Type = attributes->Get<EObjectType>("type");

            YT_LOG_DEBUG("Source object type fetched (Path: %v, Type: %v)",
                srcObject->GetPath(),
                srcObject->Type);
        }
    }

    void GetSrcObjectChunkCounts()
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());
        auto batchReq = proxy.ExecuteBatch();

        for (auto& srcObject : SrcObjects_) {
            auto req = TYPathProxy::Get(srcObject.GetObjectIdPathIfAvailable() + "/@");
            req->Tag() = &srcObject;
            AddCellTagToSyncWith(req, srcObject.ObjectId);
            NCypressClient::SetTransactionId(req, *srcObject.TransactionId);
            req->mutable_attributes()->add_keys("chunk_count");
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching source objects chunk counts");

        auto rspsOrError = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspGet>();
        for (const auto& rspOrError : rspsOrError) {
            const auto& rsp = rspOrError.Value();
            auto* srcObject = std::any_cast<TUserObject*>(rsp->Tag());

            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            auto chunkCount = attributes->Get<i64>("chunk_count");
            srcObject->ChunkCount = chunkCount;
        }
    }

    void InferOutputTableSchema()
    {
        auto createGetSchemaRequest = [&] (const TUserObject& object) {
            auto req = TYPathProxy::Get(object.GetObjectIdPathIfAvailable() + "/@");
            req->Tag() = &object;
            AddCellTagToSyncWith(req, object.ObjectId);
            NCypressClient::SetTransactionId(req, *object.TransactionId);
            req->mutable_attributes()->add_keys("schema");
            req->mutable_attributes()->add_keys("schema_mode");
            req->mutable_attributes()->add_keys("dynamic");
            return req;
        };

        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& srcObject : SrcObjects_) {
            auto req = createGetSchemaRequest(srcObject);
            batchReq->AddRequest(req, "get_src_schema");
        }
        {
            auto req = createGetSchemaRequest(DstObject_);
            batchReq->AddRequest(req, "get_dst_schema");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching table schemas");

        auto batchRsp = batchRspOrError.Value();

        std::unique_ptr<IOutputSchemaInferer> outputSchemaInferer;
        {
            const auto& rspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_dst_schema");
            YT_VERIFY(rspsOrError.size() == 1);
            const auto& rsp = rspsOrError[0].Value();

            const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            const auto schema = attributes->Get<TTableSchema>("schema");

            if (attributes->Get<bool>("dynamic")) {
                THROW_ERROR_EXCEPTION("Destination table %v is dynamic, concatenation into dynamic table is not supported",
                    DstObject_.GetPath());
            }

            const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
            switch (schemaMode) {
                case ETableSchemaMode::Strong:
                    if (schema.IsSorted()) {
                        YT_LOG_DEBUG("Using sorted concatenation (PinnedUser: %v)",
                            Client_->Options_.User);
                        Sorted_ = true;
                    }
                    outputSchemaInferer = CreateSchemaCompatibilityChecker(DstObject_.GetPath(), New<TTableSchema>(schema));
                    break;
                case ETableSchemaMode::Weak:
                    outputSchemaInferer = CreateOutputSchemaInferer();
                    if (Append_) {
                        outputSchemaInferer->AddInputTableSchema(DstObject_.GetPath(), schema, schemaMode);
                    }
                    break;
                default:
                    YT_ABORT();
            }
        }

        InputTableSchemas_.reserve(SrcObjects_.size());
        {
            const auto& rspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_src_schema");
            YT_VERIFY(rspsOrError.size() == SrcObjects_.size());
            for (const auto& rspOrError : rspsOrError) {
                const auto& rsp = rspOrError.Value();
                auto* srcObject = std::any_cast<const TUserObject*>(rsp->Tag());
                const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                const auto schema = attributes->Get<TTableSchema>("schema");
                const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");

                if (attributes->Get<bool>("dynamic")) {
                    THROW_ERROR_EXCEPTION("Source table %v is dynamic, concatenation of dynamic tables is not supported",
                        srcObject->GetPath());
                }

                outputSchemaInferer->AddInputTableSchema(srcObject->GetPath(), schema, schemaMode);

                InputTableSchemas_.push_back(schema);
            }
        }

        OutputTableSchema_ = outputSchemaInferer->GetOutputTableSchema();
        OutputTableSchemaMode_ = outputSchemaInferer->GetOutputTableSchemaMode();
    }

    void FetchChunkSpecs()
    {
        auto prepareFetchRequest = [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request, int srcObjectIndex) {
            const auto& srcObject = SrcObjects_[srcObjectIndex];

            request->set_fetch_all_meta_extensions(false);
            if (Sorted_) {
                request->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                request->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
            }
            AddCellTagToSyncWith(request, srcObject.ObjectId);
            NCypressClient::SetTransactionId(request, srcObject.ExternalTransactionId);
        };

        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            Client_,
            TMasterReadOptions{},
            Client_->Connection_->GetNodeDirectory(),
            Client_->Connection_->GetInvoker(),
            Client_->Connection_->GetConfig()->MaxChunksPerFetch,
            Client_->Connection_->GetConfig()->MaxChunksPerLocateRequest,
            prepareFetchRequest,
            Logger);

        for (int srcObjectIndex = 0; srcObjectIndex < std::ssize(SrcObjects_); ++srcObjectIndex) {
            const auto& srcObject = SrcObjects_[srcObjectIndex];

            // TODO(gritukan): Implement TVirtualTableChunkSpecFetcher.
            if (!srcObject.ObjectId) {
                continue;
            }

            chunkSpecFetcher->Add(
                srcObject.ObjectId,
                srcObject.ExternalCellTag,
                srcObject.ChunkCount,
                srcObjectIndex);
        }

        YT_LOG_DEBUG("Fetching chunk specs");

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = chunkSpecFetcher->GetChunkSpecsOrderedNaturally();

        // Fetch chunk specs of virtual tables separately.
        for (int srcObjectIndex = 0; srcObjectIndex < std::ssize(SrcObjects_); ++srcObjectIndex) {
            const auto& srcObject = SrcObjects_[srcObjectIndex];
            if (srcObject.ObjectId) {
                continue;
            }

            auto chunkSpecs = NChunkClient::FetchChunkSpecs(
                Client_,
                Client_->Connection_->GetNodeDirectory(),
                srcObject,
                /*ranges*/ {TReadRange()},
                srcObject.ChunkCount,
                Client_->Connection_->GetConfig()->MaxChunksPerFetch,
                Client_->Connection_->GetConfig()->MaxChunksPerLocateRequest,
                [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request) {
                    prepareFetchRequest(request, srcObjectIndex);
                },
                Logger);
            ChunkSpecs_.insert(ChunkSpecs_.end(), chunkSpecs.begin(), chunkSpecs.end());
        }

        YT_LOG_DEBUG("Chunk specs fetched (ChunkSpecCount: %v)",
            ChunkSpecs_.size());

        if (Options_.UniqualizeChunks) {
            std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
            chunkSpecs.reserve(ChunkSpecs_.size());

            THashSet<TChunkId> seenChunksIds;

            for (auto& chunkSpec : ChunkSpecs_) {
                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                if (seenChunksIds.insert(chunkId).second) {
                    chunkSpecs.push_back(std::move(chunkSpec));
                }
            }

            ChunkSpecs_ = std::move(chunkSpecs);

            YT_LOG_DEBUG("Chunk specs uniqualized (ChunkSpecCount: %v)",
                ChunkSpecs_.size());
        }
    }

    void ValidateChunkSchemas()
    {
        bool needChunkSchemasValidation = false;
        for (const auto& inputTableSchema : InputTableSchemas_) {
            auto schemasCompatibility = CheckTableSchemaCompatibility(
                inputTableSchema,
                *OutputTableSchema_,
                /*ignoreSortOrder*/false);
            if (schemasCompatibility.first != ESchemaCompatibility::FullyCompatible) {
                YT_LOG_DEBUG(schemasCompatibility.second,
                    "Input table schema and output table schema are incompatible; "
                    "need to validate chunk schemas");
                needChunkSchemasValidation = true;
                break;
            }
        }

        if (!needChunkSchemasValidation) {
            YT_LOG_DEBUG("Input table schemas and output table schema are compatible; "
                "skipping chunk schemas validation");
            return;
        }

        auto chunkMetaFetcher = New<TChunkMetaFetcher>(
            Options_.ChunkMetaFetcherConfig,
            Client_->Connection_->GetNodeDirectory(),
            Client_->Connection_->GetInvoker(),
            nullptr /*fetcherChunkScraper*/,
            Client_,
            Logger,
            TUserWorkloadDescriptor{EUserWorkloadCategory::Batch},
            [] (NChunkClient::NProto::TReqGetChunkMeta& request) {
                request.add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TTableSchemaExt>::Value);
            });

        for (const auto& chunkSpec : ChunkSpecs_) {
            chunkMetaFetcher->AddChunk(New<TInputChunk>(chunkSpec));
        }

        YT_LOG_DEBUG("Fetching chunk metas");

        WaitFor(chunkMetaFetcher->Fetch())
            .ThrowOnError();
        const auto& chunkMetas = chunkMetaFetcher->ChunkMetas();

        YT_LOG_DEBUG("Chunk metas fetched (ChunkMetaCount: %v)",
            chunkMetas.size());

        YT_VERIFY(ChunkSpecs_.size() == chunkMetas.size());

        YT_LOG_DEBUG("Validating chunks schemas");

        for (int chunkIndex = 0; chunkIndex < std::ssize(ChunkSpecs_); ++chunkIndex) {
            const auto& chunkMeta = chunkMetas[chunkIndex];
            const auto& chunkSpec = ChunkSpecs_[chunkIndex];
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());

            if (!chunkMeta) {
                THROW_ERROR_EXCEPTION("Chunk %v meta was not fetched", chunkId);
            }

            auto chunkSchemaExt =
                FindProtoExtension<NTableClient::NProto::TTableSchemaExt>(chunkMeta->extensions());
            if (!chunkSchemaExt) {
                THROW_ERROR_EXCEPTION("Chunk %v does not have schema extension in meta",
                    chunkId);
            }

            auto chunkSchema = FromProto<TTableSchema>(*chunkSchemaExt);

            if (OutputTableSchema_->GetKeyColumnCount() > chunkSchema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
                    "Chunk %v has less key columns than output schema",
                    chunkId)
                    << TErrorAttribute("chunk_key_column_count", chunkSchema.GetKeyColumnCount())
                    << TErrorAttribute("output_table_key_column_count", OutputTableSchema_->GetKeyColumnCount());
            }

            if (OutputTableSchema_->GetUniqueKeys() && !chunkSchema.GetUniqueKeys()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::SchemaViolation,
                    "Output table schema forces keys to be unique while chunk %v schema does not",
                    chunkId);
            }
        }
    }

    void OrderChunks()
    {
        YT_LOG_DEBUG("Sorting chunks");

        auto comparator = OutputTableSchema_->ToComparator();
        std::stable_sort(
            ChunkSpecs_.begin(),
            ChunkSpecs_.end(),
            [&] (const NChunkClient::NProto::TChunkSpec& lhs, const NChunkClient::NProto::TChunkSpec& rhs) {
                auto [lhsMinKey, lhsMaxKey] = GetChunkBoundaryKeys(lhs.chunk_meta());
                auto [rhsMinKey, rhsMaxKey] = GetChunkBoundaryKeys(rhs.chunk_meta());

                int minKeyResult = comparator.CompareKeys(lhsMinKey, rhsMinKey);
                if (minKeyResult != 0) {
                    return minKeyResult < 0;
                } else {
                    return comparator.CompareKeys(lhsMaxKey, rhsMaxKey) < 0;
                }
            });
    }

    void ValidateChunkRanges()
    {
        YT_LOG_DEBUG("Validating chunk ranges");

        auto comparator = OutputTableSchema_->ToComparator();
        for (int chunkIndex = 0; chunkIndex + 1 < std::ssize(ChunkSpecs_); ++chunkIndex) {
            const auto& currentChunkSpec = ChunkSpecs_[chunkIndex];
            const auto& nextChunkSpec = ChunkSpecs_[chunkIndex + 1];

            auto currentChunkMaxKey = GetChunkBoundaryKeys(currentChunkSpec.chunk_meta()).second;
            auto nextChunkMinKey = GetChunkBoundaryKeys(nextChunkSpec.chunk_meta()).first;

            int comparisonResult = comparator.CompareKeys(currentChunkMaxKey, nextChunkMinKey);
            if (comparisonResult > 0) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SortOrderViolation, "Chunks ranges are overlapping")
                    << TErrorAttribute("current_chunk_id", FromProto<TChunkId>(currentChunkSpec.chunk_id()))
                    << TErrorAttribute("next_chunk_id", FromProto<TChunkId>(nextChunkSpec.chunk_id()))
                    << TErrorAttribute("current_chunk_max_key", currentChunkMaxKey)
                    << TErrorAttribute("next_chunk_min_key", nextChunkMinKey)
                    << TErrorAttribute("comparator", comparator);
            } else if (comparisonResult == 0 && OutputTableSchema_->GetUniqueKeys()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::UniqueKeyViolation,
                    "Key appears in two chunks but output table schema requires unique keys")
                    << TErrorAttribute("current_chunk_id", FromProto<TChunkId>(currentChunkSpec.chunk_id()))
                    << TErrorAttribute("next_chunk_id", FromProto<TChunkId>(nextChunkSpec.chunk_id()))
                    << TErrorAttribute("current_chunk_max_key", currentChunkMaxKey)
                    << TErrorAttribute("next_chunk_min_key", nextChunkMinKey)
                    << TErrorAttribute("comparator", comparator);
            }
        }
    }

    void ValidateBoundaryKeys()
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());

        auto request = TTableYPathProxy::Get(DstObject_.GetObjectIdPath() + "/@boundary_keys");
        AddCellTagToSyncWith(request, DstObject_.ObjectId);
        NCypressClient::SetTransactionId(request, *DstObject_.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(request));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to fetch boundary keys of destination table %v",
            DstObject_.GetPath());

        auto boundaryKeysMap = ConvertToNode(TYsonString(rspOrError.Value()->value()))->AsMap();
        auto tableMaxKeyNode = boundaryKeysMap->FindChild("max_key");

        if (tableMaxKeyNode && !ChunkSpecs_.empty()) {
            auto comparator = OutputTableSchema_->ToComparator();

            auto tableMaxKeyRow = ConvertTo<TUnversionedOwningRow>(tableMaxKeyNode);

            auto fixedTableMaxKeyRow = LegacyKeyToKeyFriendlyOwningRow(tableMaxKeyRow, comparator.GetLength());
            if (tableMaxKeyRow != fixedTableMaxKeyRow) {
                YT_LOG_DEBUG(
                    "Table max key fixed (MaxKey: %v -> %v)",
                    tableMaxKeyRow,
                    fixedTableMaxKeyRow);
                tableMaxKeyRow = fixedTableMaxKeyRow;
            }

            YT_VERIFY(tableMaxKeyRow.GetCount() == comparator.GetLength());
            auto tableMaxKey = TKey::FromRow(tableMaxKeyRow);

            YT_LOG_DEBUG(
                "Writing to table in sorted append mode (MaxKey: %v)",
                tableMaxKey);

            auto firstChunkMinKey = GetChunkBoundaryKeys(ChunkSpecs_[0].chunk_meta()).first;

            YT_LOG_DEBUG(
                "Comparing table max key against first chunk min key (MaxKey: %v, MinKey: %v, Comparator: %v)",
                tableMaxKey,
                firstChunkMinKey,
                comparator);

            auto comparisonResult = comparator.CompareKeys(tableMaxKey, firstChunkMinKey);
            if (comparisonResult > 0) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::SortOrderViolation,
                    "First key of chunk to append is less than last key in table")
                    << TErrorAttribute("chunk_id", FromProto<TChunkId>(ChunkSpecs_[0].chunk_id()))
                    << TErrorAttribute("table_max_key", tableMaxKey)
                    << TErrorAttribute("first_chunk_min_key", firstChunkMinKey)
                    << TErrorAttribute("comparator", comparator);
            } else if (comparisonResult == 0 && OutputTableSchema_->GetUniqueKeys()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::UniqueKeyViolation,
                    "First key of chunk to append equals to last key in table")
                    << TErrorAttribute("chunk_id", FromProto<TChunkId>(ChunkSpecs_[0].chunk_id()))
                    << TErrorAttribute("table_max_key", tableMaxKey)
                    << TErrorAttribute("first_chunk_min_key", firstChunkMinKey)
                    << TErrorAttribute("comparator", comparator);
            }
        }
    }

    void BeginUpload()
    {
        auto dstObjectCellTag = CellTagFromId(DstObject_.ObjectId);
        auto proxy = Client_->CreateObjectServiceWriteProxy(dstObjectCellTag);

        auto req = TChunkOwnerYPathProxy::BeginUpload(DstObject_.GetObjectIdPath());
        req->set_update_mode(static_cast<int>(Append_ ? EUpdateMode::Append : EUpdateMode::Overwrite));
        req->set_lock_mode(static_cast<int>(Append_ ? ELockMode::Shared : ELockMode::Exclusive));
        if (CommonType_ == EObjectType::Table) {
            ToProto(req->mutable_table_schema(), OutputTableSchema_);
            req->set_schema_mode(static_cast<int>(OutputTableSchemaMode_));
        }

        std::vector<TString> srcObjectPaths;
        srcObjectPaths.reserve(SrcObjects_.size());
        for (const auto& srcObject : SrcObjects_) {
            srcObjectPaths.push_back(srcObject.GetPath());
        }
        req->set_upload_transaction_title(Format("Concatenating %v to %v",
            srcObjectPaths,
            DstObject_.GetPath()));

        auto cellTags = GetAffectedCellTags();
        cellTags.erase(
            std::remove(cellTags.begin(), cellTags.end(), dstObjectCellTag),
            cellTags.end());
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(ToProto<i64>(Client_->Connection_->GetConfig()->UploadTransactionTimeout));
        NRpc::GenerateMutationId(req);
        Client_->SetTransactionId(req, Options_, true);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting upload to %v",
            srcObjectPaths);
        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        UploadTransaction_ = Client_->TransactionManager_->Attach(uploadTransactionId, TTransactionAttachOptions{
            .AutoAbort = true,
            .PingAncestors = Options_.PingAncestors
        });
    }

    TCellTagList GetAffectedCellTags()
    {
        THashSet<TCellTag> cellTags;

        for (const auto& chunkSpec : ChunkSpecs_) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto cellTag = CellTagFromId(chunkId);
            cellTags.insert(cellTag);
        }

        cellTags.insert(DstObject_.ExternalCellTag);

        return {cellTags.begin(), cellTags.end()};
    }

    void TeleportChunks()
    {
        auto teleporter = New<TChunkTeleporter>(
            Client_->Connection_->GetConfig(),
            Client_,
            Client_->Connection_->GetInvoker(),
            UploadTransaction_->GetId(),
            Logger);

        for (const auto& chunkSpec : ChunkSpecs_) {
            teleporter->RegisterChunk(FromProto<TChunkId>(chunkSpec.chunk_id()), DstObject_.ExternalCellTag);
        }

        WaitFor(teleporter->Run())
            .ThrowOnError();
    }

    void GetUploadParams()
    {
        auto proxy = Client_->CreateObjectServiceWriteProxy(DstObject_.ExternalCellTag);

        auto req = TChunkOwnerYPathProxy::GetUploadParams(DstObject_.GetObjectIdPath());
        NCypressClient::SetTransactionId(req, UploadTransaction_->GetId());

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting upload parameters for %v",
            DstObject_.GetPath());
        const auto& rsp = rspOrError.Value();
        ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    void UploadChunks()
    {
        auto proxy = Client_->CreateWriteProxy<TChunkServiceProxy>(DstObject_.ExternalCellTag);

        auto batchReq = proxy.ExecuteBatch();
        NRpc::GenerateMutationId(batchReq);
        SetSuppressUpstreamSync(&batchReq->Header(), true);
        // COMPAT(shakurov): prefer proto ext (above).
        batchReq->set_suppress_upstream_sync(true);

        auto req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), ChunkListId_);

        for (const auto& chunkSpec : ChunkSpecs_) {
            *req->add_child_ids() = chunkSpec.chunk_id();
        }
        req->set_request_statistics(true);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to %v",
            DstObject_.GetPath());
        const auto& batchRsp = batchRspOrError.Value();

        const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);
        DataStatistics_ = rsp.statistics();
    }

    void EndUpload()
    {
        auto proxy = Client_->CreateObjectServiceWriteProxy(CellTagFromId(DstObject_.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(DstObject_.GetObjectIdPath());
        *req->mutable_statistics() = DataStatistics_;

        // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
        if (CommonType_ == EObjectType::Table) {
            ToProto(req->mutable_table_schema(), OutputTableSchema_);
            req->set_schema_mode(static_cast<int>(OutputTableSchemaMode_));
        }

        std::vector<TSecurityTag> inferredSecurityTags;
        for (const auto& srcObject : SrcObjects_) {
            inferredSecurityTags.insert(inferredSecurityTags.end(), srcObject.SecurityTags.begin(), srcObject.SecurityTags.end());
        }
        SortUnique(inferredSecurityTags);
        YT_LOG_DEBUG("Security tags inferred (SecurityTags: %v)",
            inferredSecurityTags);

        std::vector<TSecurityTag> securityTags;
        if (auto explicitSecurityTags = DstObject_.Path.GetSecurityTags()) {
            // TODO(babenko): audit
            YT_LOG_INFO("Destination table is assigned explicit security tags (Path: %v, InferredSecurityTags: %v, ExplicitSecurityTags: %v)",
                DstObject_.GetPath(),
                inferredSecurityTags,
                explicitSecurityTags);
            securityTags = *explicitSecurityTags;
        } else {
            YT_LOG_INFO("Destination table is assigned automatically-inferred security tags (Path: %v, SecurityTags: %v)",
                DstObject_.GetPath(),
                inferredSecurityTags);
            securityTags = inferredSecurityTags;
        }

        ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
        NCypressClient::SetTransactionId(req, UploadTransaction_->GetId());
        NRpc::GenerateMutationId(req);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error finishing upload to %v",
            DstObject_.GetPath());

        UploadTransaction_->Detach();
    }

    std::pair<TKey, TKey> GetChunkBoundaryKeys(const NChunkClient::NProto::TChunkMeta& chunkMeta)
    {
        int keyColumnCount = OutputTableSchema_->GetKeyColumnCount();

        auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(chunkMeta.extensions());
        auto minKeyRow = FromProto<TUnversionedOwningRow>(boundaryKeysExt.min());
        auto maxKeyRow = FromProto<TUnversionedOwningRow>(boundaryKeysExt.max());

        auto prepareKey = [&] (TUnversionedOwningRow row) {
            // NB: This can happen due to altering a key column.
            if (row.GetCount() < keyColumnCount) {
                row = WidenKey(row, keyColumnCount);
            }

            return TKey::FromRowUnchecked(RowBuffer_->CaptureRow(MakeRange(row.Begin(), keyColumnCount)), keyColumnCount);
        };

        return {prepareKey(minKeyRow), prepareKey(maxKeyRow)};
    }
};

void TClient::DoConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    if (options.Retry) {
        THROW_ERROR_EXCEPTION("\"concatenate\" command is not retriable");
    }

    TNodeConcatenator{MakeStrong(this), Logger}
        .ConcatenateNodes(srcPaths, dstPath, options);
}

bool TClient::DoNodeExists(
    const TYPath& path,
    const TNodeExistsOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->NodeExists(path, options)) {
            return *result;
        }
    }
    YT_ABORT();
}

void TClient::DoExternalizeNode(
    const TYPath& path,
    TCellTag cellTag,
    const TExternalizeNodeOptions& options)
{
    TNodeExternalizer externalizer(
        this,
        path,
        cellTag,
        options,
        Logger);
    return externalizer.Run();
}

void TClient::DoInternalizeNode(
    const TYPath& path,
    const TInternalizeNodeOptions& options)
{
    TNodeInternalizer internalizer(
        this,
        path,
        options,
        Logger);
    return internalizer.Run();
}

TObjectId TClient::DoCreateObject(
    EObjectType type,
    const TCreateObjectOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->CreateObject(type, options)) {
            return *result;
        }
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
