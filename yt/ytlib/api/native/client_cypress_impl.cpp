#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"

#include <yt/client/object_client/helpers.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_meta_fetcher.h>
#include <yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/input_chunk.h>
#include <yt/ytlib/chunk_client/fetcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/schema_inferer.h>

#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/ytlib/tablet_client/helpers.h>

#include <yt/core/ypath/helpers.h>
#include <yt/core/ypath/tokenizer.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NSecurityClient;
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool TryParseObjectId(const TYPath& path, TObjectId* objectId)
{
    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() != NYPath::ETokenType::Literal) {
        return false;
    }

    auto token = tokenizer.GetToken();
    if (!token.StartsWith(ObjectIdPathPrefix)) {
        return false;
    }

    *objectId = TObjectId::FromString(token.SubString(
        ObjectIdPathPrefix.length(),
        token.length() - ObjectIdPathPrefix.length()));
    return true;
}

template <class TRequestPtr>
void SetCloneNodeBaseRequestParameters(
    const TRequestPtr& req,
    const TCopyNodeOptionsBase& options)
{
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_creation_time(options.PreserveCreationTime);
    req->set_preserve_modification_time(options.PreserveModificationTime);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);
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

    struct TSerializedSubtree
    {
        TSerializedSubtree() = default;

        TSerializedSubtree(TYPath path, NCypressClient::NProto::TSerializedTree serializedValue)
            : Path(std::move(path))
            , SerializedValue(std::move(serializedValue))
        { }

        //! Relative to tree root path to subtree root.
        TYPath Path;

        //! Serialized subtree.
        NCypressClient::NProto::TSerializedTree SerializedValue;
    };

    const TClientPtr Client_;

    const NLogging::TLogger Logger;

    ITransactionPtr Transaction_;

    std::vector<TSerializedSubtree> SerializedSubtrees_;

    TNodeId SrcNodeId_;
    TNodeId DstNodeId_;

    std::vector<TCellTag> ExternalCellTags_;


    template <class TOptions>
    void StartTransaction(const TString& title, const TOptions& options)
    {
        YT_LOG_DEBUG("Starting transaction");

        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", title);

        auto transactionOrError = WaitFor(Client_->StartNativeTransaction(
            ETransactionType::Master,
            TTransactionStartOptions{
                .ParentId = options.TransactionId,
                .Attributes = std::move(transactionAttributes)
            }));
        THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting transaction");
        Transaction_ = transactionOrError.Value();

        YT_LOG_DEBUG("Transaction started (TransactionId: %v)",
            Transaction_->GetId());
    }

    template <class TOptions>
    void BeginCopy(const TYPath& srcPath, const TOptions& options)
    {
        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(std::move(channel));

        std::queue<TYPath> subtreeSerializationQueue;
        subtreeSerializationQueue.push(srcPath);

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
            if (subtreePath == srcPath) {
                SrcNodeId_ = nodeId;
            }

            YT_LOG_DEBUG("Serialized subtree received (NodeId: %v, Path: %v, FormatVersion: %v, TreeSize: %v, "
                 "PortalChildIds: %v, ExternalCellTags: %v, OpaqueChildPaths: %v)",
                 nodeId,
                 subtreePath,
                 rsp->serialized_tree().version(),
                 rsp->serialized_tree().data().size(),
                 portalChildIds,
                 externalCellTags,
                 opaqueChildPaths);

            auto relativePath = TryComputeYPathSuffix(subtreePath, srcPath);
            YT_VERIFY(relativePath);

            SerializedSubtrees_.emplace_back(*relativePath, std::move(*rsp->mutable_serialized_tree()));

            for (auto cellTag : externalCellTags) {
                ExternalCellTags_.push_back(cellTag);
            }

            for (const auto& opaqueChildPath : opaqueChildPaths) {
                subtreeSerializationQueue.push(opaqueChildPath);
            }
        }

        SortUnique(ExternalCellTags_);
    }

    template <class TOptions>
    void EndCopy(const TYPath& dstPath, const TOptions& options, bool inplace)
    {
        YT_LOG_DEBUG("Materializing serialized subtrees");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(std::move(channel));

        for (auto& subtree : SerializedSubtrees_) {
            auto batchReq = proxy.ExecuteBatch();
            auto req = TCypressYPathProxy::EndCopy(dstPath + subtree.Path);
            GenerateMutationId(req);
            SetTransactionId(req, Transaction_->GetId());
            SetEndCopyNodeRequestParameters(req, options);
            req->set_inplace(inplace);
            *req->mutable_serialized_tree() = std::move(subtree.SerializedValue);
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

    void SyncExternalCellsWithClonedNodeCell()
    {
        if (ExternalCellTags_.empty()) {
            return;
        }

        YT_LOG_DEBUG("Synchronizing external cells with the cloned node cell");

        auto nodeCellTag = CellTagFromId(DstNodeId_);
        const auto& connection = Client_->GetNativeConnection();
        std::vector<TFuture<void>> futures;
        for (auto externalCellTag : ExternalCellTags_) {
            futures.push_back(connection->SyncHiveCellWithOthers(
                {connection->GetMasterCellId(nodeCellTag)},
                connection->GetMasterCellId(externalCellTag)));
        }

        auto error = WaitFor(Combine(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing external cells with the cloned node cell");

        YT_LOG_DEBUG("External cells synchronized with the cloned node cell");
    }

    void CommitTransaction()
    {
        YT_LOG_DEBUG("Committing transaction");

        auto error = WaitFor(Transaction_->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error committing transaction");

        YT_LOG_DEBUG("Transaction committed");
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
        NLogging::TLogger logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.AddTag("SrcPath: %v, DstPath: %v",
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
        BeginCopy(SrcPath_, Options_);
        EndCopy(DstPath_, Options_, false);
        if constexpr(std::is_assignable_v<TOptions, TMoveNodeOptions>) {
            RemoveSource();
        }
        SyncExternalCellsWithClonedNodeCell();
        CommitTransaction();
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
        NLogging::TLogger logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.AddTag("Path: %v, CellTag: %v",
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
        RequestRootEffectiveAcl();
        BeginCopy(Path_, GetOptions());
        if (TypeFromId(SrcNodeId_) != EObjectType::MapNode) {
            THROW_ERROR_EXCEPTION("%v is not a map node", Path_);
        }
        CreatePortal();
        SyncExitCellWithEntranceCell();
        EndCopy(Path_, GetOptions(), true);
        SyncExternalCellsWithClonedNodeCell();
        CommitTransaction();
        YT_LOG_DEBUG("Node externalization completed");
    }

private:
    const TYPath Path_;
    const TCellTag CellTag_;
    const TExternalizeNodeOptions Options_;

    TYsonString RootEffectiveAcl_;


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

    void RequestRootEffectiveAcl()
    {
        YT_LOG_DEBUG("Requesting root effective ACL");

        auto aclOrError = WaitFor(Transaction_->GetNode(Path_ + "/@effective_acl"));
        THROW_ERROR_EXCEPTION_IF_FAILED(aclOrError, "Error getting root effective ACL");
        RootEffectiveAcl_ = aclOrError.Value();

        YT_LOG_DEBUG("Root effective ACL received");
    }

    void CreatePortal()
    {
        YT_LOG_DEBUG("Creating portal");

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("exit_cell_tag", CellTag_);
        attributes->Set("inherit_acl", false);
        attributes->Set("acl", RootEffectiveAcl_);

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
        NLogging::TLogger logger)
        : TCrossCellExecutor(
            std::move(client),
            logger.AddTag("Path: %v",
                path))
        , Path_(std::move(path))
        , Options_(options)
    { }

    void Run()
    {
        YT_LOG_DEBUG("Node internalization started");
        StartTransaction(
            Format("Internalize %v", Path_),
            Options_);
        BeginCopy(Path_, GetOptions());
        if (TypeFromId(SrcNodeId_) != EObjectType::PortalExit) {
            THROW_ERROR_EXCEPTION("%v is not a portal", Path_);
        }
        CreateMapNode();
        EndCopy(Path_ + "&", GetOptions(), true);
        SyncExternalCellsWithClonedNodeCell();
        CommitTransaction();
        YT_LOG_DEBUG("Node internalization completed");
    }

private:
    const TYPath Path_;
    const TInternalizeNodeOptions Options_;


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

    void CreateMapNode()
    {
        YT_LOG_DEBUG("Creating map node");

        TCreateNodeOptions options;
        options.Force = true;

        auto nodeIdOrError = WaitFor(Transaction_->CreateNode(
            Path_ + "&",
            EObjectType::MapNode,
            options));
        THROW_ERROR_EXCEPTION_IF_FAILED(nodeIdOrError, "Error creating map node");

        YT_LOG_DEBUG("Map node created");
    }
};

} // namespace

TYsonString TClient::DoGetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TYPathProxy::Get(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetCachingHeader(req, options);
    if (options.Attributes) {
        ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
    }
    if (options.MaxSize) {
        req->set_limit(*options.MaxSize);
    }
    if (options.Options) {
        ToProto(req->mutable_options(), *options.Options);
    }
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0)
        .ValueOrThrow();

    return TYsonString(rsp->value());
}

void TClient::DoSetNode(
    const TYPath& path,
    const TYsonString& value,
    const TSetNodeOptions& options)
{
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TYPathProxy::Set(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetMutationId(req, options);

    // Binarize the value.
    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream, EYsonType::Node, false);
    YT_VERIFY(value.GetType() == EYsonType::Node);
    writer.OnRaw(value.GetData(), EYsonType::Node);
    writer.Flush();
    req->set_value(stream.Str());
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TYPathProxy::TRspSet>(0)
        .ThrowOnError();
}

void TClient::DoRemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    auto cellTag = PrimaryMasterCellTag;

    TObjectId objectId;
    if (TryParseObjectId(path, &objectId)) {
        cellTag = CellTagFromId(objectId);
        switch (TypeFromId(objectId)) {
            case EObjectType::TableReplica: {
                InternalValidateTableReplicaPermission(objectId, EPermission::Write);
                break;
            }
            default:
                break;
        }
    }

    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TYPathProxy::Remove(path);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TYPathProxy::TRspRemove>(0)
        .ThrowOnError();
}

TYsonString TClient::DoListNode(
    const TYPath& path,
    const TListNodeOptions& options)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TYPathProxy::List(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetCachingHeader(req, options);
    if (options.Attributes) {
        ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
    }
    if (options.MaxSize) {
        req->set_limit(*options.MaxSize);
    }
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>(0)
        .ValueOrThrow();

    return TYsonString(rsp->value());
}

TNodeId TClient::DoCreateNode(
    const TYPath& path,
    EObjectType type,
    const TCreateNodeOptions& options)
{
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();
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
    if (options.Attributes) {
        ToProto(req->mutable_node_attributes(), *options.Attributes);
    }
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0)
        .ValueOrThrow();
    return FromProto<TNodeId>(rsp->node_id());
}

TLockNodeResult TClient::DoLockNode(
    const TYPath& path,
    ELockMode mode,
    const TLockNodeOptions& options)
{
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();

    auto batchReqConfig = New<TReqExecuteBatchWithRetriesConfig>();
    batchReqConfig->RetriableErrorCodes.push_back(
        static_cast<TErrorCode::TUnderlying>(NTabletClient::EErrorCode::InvalidTabletState));
    auto batchReq = proxy->ExecuteBatchWithRetries(std::move(batchReqConfig));

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
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();
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
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Copy(dstPath);
    SetCopyNodeRequestParameters(req, options);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    // COMPAT(babenko)
    req->set_source_path(srcPath);
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

    if (!options.PrerequisiteTransactionIds.empty() ||
        !options.PrerequisiteRevisions.empty())
    {
        THROW_ERROR_EXCEPTION("Cross-cell \"copy\"/\"move\" command does not support prerequisites");
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
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Create(dstPath);
    req->set_type(static_cast<int>(EObjectType::Link));
    req->set_recursive(options.Recursive);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
    req->set_force(options.Force);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    auto attributes = options.Attributes ? ConvertToAttributes(options.Attributes.get()) : CreateEphemeralAttributes();
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

    TTableSchema OutputTableSchema_;
    ETableSchemaMode OutputTableSchemaMode_;

    TTransactionPtr UploadTransaction_;

    TChunkListId ChunkListId_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;

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

        auto transactionsOrError = WaitFor(Combine(asyncTransactions));
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
        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
        auto batchReq = proxy->ExecuteBatch();

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
                srcObject->ExternalCellTag = rsp->external_cell_tag();
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
            dstObject->ExternalCellTag = rsp->external_cell_tag();

            YT_LOG_DEBUG("Destination object attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v)",
                dstObject->GetPath(),
                dstObject->ObjectId,
                dstObject->GetObjectIdPath());
        }
    }

    void GetCommonType()
    {
        std::optional<EObjectType> commonType;
        TString pathWithCommonType;

        auto checkType = [&] (const TUserObject& object) {
            auto type = TypeFromId(object.ObjectId);
            if (type != EObjectType::Table && type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Type of %v must be either %Qlv or %Qlv",
                    object.GetPath(),
                    EObjectType::Table,
                    EObjectType::File);
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

    void GetSrcObjectChunkCounts()
    {
        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
        auto batchReq = proxy->ExecuteBatch();

        for (auto& srcObject : SrcObjects_) {
            auto req = TYPathProxy::Get(srcObject.GetObjectIdPath() + "/@");
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
            auto req = TYPathProxy::Get(object.GetObjectIdPath() + "/@");
            req->Tag() = &object;
            AddCellTagToSyncWith(req, object.ObjectId);
            NCypressClient::SetTransactionId(req, *object.TransactionId);
            req->mutable_attributes()->add_keys("schema");
            req->mutable_attributes()->add_keys("schema_mode");
            req->mutable_attributes()->add_keys("dynamic");
            return req;
        };

        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
        auto batchReq = proxy->ExecuteBatch();
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
                            Client_->Options_.PinnedUser);
                        Sorted_ = true;
                    }
                    outputSchemaInferer = CreateSchemaCompatibilityChecker(DstObject_.GetPath(), schema);
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
            }
        }

        OutputTableSchema_ = outputSchemaInferer->GetOutputTableSchema();
        OutputTableSchemaMode_ = outputSchemaInferer->GetOutputTableSchemaMode();
    }

    void FetchChunkSpecs()
    {
        auto chunkSpecFetcher = New<TChunkSpecFetcher>(
            Client_,
            Client_->Connection_->GetNodeDirectory(),
            Client_->Connection_->GetInvoker(),
            Client_->Connection_->GetConfig()->MaxChunksPerFetch,
            Client_->Connection_->GetConfig()->MaxChunksPerLocateRequest,
            [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request, int srcObjectIndex) {
                const auto& srcObject = SrcObjects_[srcObjectIndex];

                request->set_fetch_all_meta_extensions(false);
                if (Sorted_) {
                    request->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                    request->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                }
                AddCellTagToSyncWith(request, srcObject.ObjectId);
                NCypressClient::SetTransactionId(request, srcObject.ExternalTransactionId);
            },
            Logger);

        for (int srcObjectIndex = 0; srcObjectIndex < SrcObjects_.size(); ++srcObjectIndex) {
            const auto& srcObject = SrcObjects_[srcObjectIndex];

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

        YT_LOG_DEBUG("Chunk specs fetched (ChunkSpecCount: %v)",
            ChunkSpecs_.size());
    }

    void ValidateChunkSchemas()
    {
        auto chunkMetaFetcher = New<TChunkMetaFetcher>(
            Options_.ChunkMetaFetcherConfig,
            Client_->Connection_->GetNodeDirectory(),
            Client_->Connection_->GetInvoker(),
            nullptr /* fetcherChunkScraper */,
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

        YT_LOG_DEBUG("Chunk metas fecthed (ChunkMetaCount: %v)",
            chunkMetas.size());

        YT_VERIFY(ChunkSpecs_.size() == chunkMetas.size());

        YT_LOG_DEBUG("Validating chunks schemas");

        for (int chunkIndex = 0; chunkIndex < ChunkSpecs_.size(); ++chunkIndex) {
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

            if (OutputTableSchema_.GetKeyColumnCount() > chunkSchema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
                    "Chunk %v has less key columns than output schema",
                    chunkId)
                    << TErrorAttribute("chunk_key_column_count", chunkSchema.GetKeyColumnCount())
                    << TErrorAttribute("output_table_key_column_count", OutputTableSchema_.GetKeyColumnCount());
            }

            if (OutputTableSchema_.GetUniqueKeys() && !chunkSchema.GetUniqueKeys()) {
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

        std::stable_sort(
            ChunkSpecs_.begin(),
            ChunkSpecs_.end(),
            [&] (const NChunkClient::NProto::TChunkSpec& lhs, const NChunkClient::NProto::TChunkSpec& rhs) {
                auto lhsBoundaryKeysExt =
                    FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(lhs.chunk_meta().extensions());
                auto rhsBoundaryKeysExt =
                    FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(rhs.chunk_meta().extensions());
                auto lhsMinKey = FromProto<TOwningKey>(lhsBoundaryKeysExt->min());
                auto rhsMinKey = FromProto<TOwningKey>(rhsBoundaryKeysExt->min());

                int compareResult = CompareRows(lhsMinKey, rhsMinKey, OutputTableSchema_.GetKeyColumnCount());
                if (compareResult < 0) {
                    return true;
                } else if (compareResult > 0) {
                    return false;
                } else {
                    auto lhsMaxKey = FromProto<TOwningKey>(lhsBoundaryKeysExt->max());
                    auto rhsMaxKey = FromProto<TOwningKey>(rhsBoundaryKeysExt->max());

                    return CompareRows(lhsMaxKey, rhsMaxKey, OutputTableSchema_.GetKeyColumnCount()) < 0;
                }
            });
    }

    void ValidateChunkRanges()
    {
        YT_LOG_DEBUG("Validating chunk ranges");

        for (int chunkIndex = 0; chunkIndex + 1 < ChunkSpecs_.size(); ++chunkIndex) {
            const auto& currentChunkSpec = ChunkSpecs_[chunkIndex];
            const auto& nextChunkSpec = ChunkSpecs_[chunkIndex + 1];

            auto currentChunkMaxKey = FromProto<TOwningKey>(
                FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
                    currentChunkSpec.chunk_meta().extensions())->max());
            auto nextChunkMinKey = FromProto<TOwningKey>(
                FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
                    nextChunkSpec.chunk_meta().extensions())->min());

            int compareResult = CompareRows(
                currentChunkMaxKey,
                nextChunkMinKey,
                OutputTableSchema_.GetKeyColumnCount());

            if (compareResult > 0) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SortOrderViolation, "Chunks ranges are overlapping")
                    << TErrorAttribute("current_chunk_id", FromProto<TChunkId>(currentChunkSpec.chunk_id()))
                    << TErrorAttribute("next_chunk_id", FromProto<TChunkId>(nextChunkSpec.chunk_id()))
                    << TErrorAttribute("current_chunk_max_key", currentChunkMaxKey)
                    << TErrorAttribute("next_chunk_min_key", nextChunkMinKey)
                    << TErrorAttribute("key_column_count", OutputTableSchema_.GetKeyColumnCount());
            }

            if (compareResult == 0 && OutputTableSchema_.GetUniqueKeys()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::UniqueKeyViolation,
                    "Key appears in two chunks but output table schema requires unique keys")
                    << TErrorAttribute("current_chunk_id", FromProto<TChunkId>(currentChunkSpec.chunk_id()))
                    << TErrorAttribute("next_chunk_id", FromProto<TChunkId>(nextChunkSpec.chunk_id()))
                    << TErrorAttribute("current_chunk_max_key", currentChunkMaxKey)
                    << TErrorAttribute("next_chunk_min_key", nextChunkMinKey)
                    << TErrorAttribute("key_column_count", OutputTableSchema_.GetKeyColumnCount());
            }
        }
    }

    void ValidateBoundaryKeys()
    {
        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());

        auto request = TTableYPathProxy::Get(DstObject_.GetObjectIdPath() + "/@boundary_keys");
        AddCellTagToSyncWith(request, DstObject_.ObjectId);
        NCypressClient::SetTransactionId(request, *DstObject_.TransactionId);

        auto rspOrError = WaitFor(proxy->Execute(request));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to fetch boundary keys of destination table %v",
            DstObject_.GetPath());

        auto boundaryKeysMap = ConvertToNode(TYsonString(rspOrError.Value()->value()))->AsMap();
        auto maxKeyNode = boundaryKeysMap->FindChild("max_key");

        if (maxKeyNode && !ChunkSpecs_.empty()) {
            auto maxKey = ConvertTo<TOwningKey>(maxKeyNode);

            auto firstChunkMinKey = FromProto<TOwningKey>(
                FindProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
                ChunkSpecs_[0].chunk_meta().extensions())->min());

            auto compareResult = CompareRows(
                maxKey,
                firstChunkMinKey,
                OutputTableSchema_.GetKeyColumnCount());

            if (compareResult > 0) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::SortOrderViolation,
                    "First key of chunk to append is less than last key in table")
                    << TErrorAttribute("chunk_id", FromProto<TChunkId>(ChunkSpecs_[0].chunk_id()))
                    << TErrorAttribute("table_max_key", maxKey)
                    << TErrorAttribute("first_chunk_min_key", firstChunkMinKey)
                    << TErrorAttribute("key_column_count", OutputTableSchema_.GetKeyColumnCount());
            }

            if (compareResult == 0 && OutputTableSchema_.GetUniqueKeys()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::UniqueKeyViolation,
                    "First key of chunk to append equals to last key in table")
                    << TErrorAttribute("chunk_id", FromProto<TChunkId>(ChunkSpecs_[0].chunk_id()))
                    << TErrorAttribute("table_max_key", maxKey)
                    << TErrorAttribute("first_chunk_min_key", firstChunkMinKey)
                    << TErrorAttribute("key_column_count", OutputTableSchema_.GetKeyColumnCount());
            }
        }
    }

    void BeginUpload()
    {
        auto proxy = Client_->CreateWriteProxy<TObjectServiceProxy>(CellTagFromId(DstObject_.ObjectId));

        auto req = TChunkOwnerYPathProxy::BeginUpload(DstObject_.GetObjectIdPath());
        req->set_update_mode(static_cast<int>(Append_ ? EUpdateMode::Append : EUpdateMode::Overwrite));
        req->set_lock_mode(static_cast<int>(Append_ ? ELockMode::Shared : ELockMode::Exclusive));

        std::vector<TString> srcObjectPaths;
        srcObjectPaths.reserve(SrcObjects_.size());
        for (const auto& srcObject : SrcObjects_) {
            srcObjectPaths.push_back(srcObject.GetPath());
        }
        req->set_upload_transaction_title(Format("Concatenating %v to %v",
            srcObjectPaths,
            DstObject_.GetPath()));
        // NB: Replicate upload transaction to each secondary cell since we have
        // no idea as of where the chunks we're about to attach may come from.
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), Client_->Connection_->GetSecondaryMasterCellTags());
        req->set_upload_transaction_timeout(ToProto<i64>(Client_->Connection_->GetConfig()->UploadTransactionTimeout));
        NRpc::GenerateMutationId(req);
        Client_->SetTransactionId(req, Options_, true);

        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting upload to %v",
            srcObjectPaths);
        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        UploadTransaction_ = Client_->TransactionManager_->Attach(uploadTransactionId, TTransactionAttachOptions{
            .AutoAbort = true,
            .PingAncestors = Options_.PingAncestors
        });
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
        auto proxy = Client_->CreateWriteProxy<TObjectServiceProxy>(DstObject_.ExternalCellTag);

        auto req = TChunkOwnerYPathProxy::GetUploadParams(DstObject_.GetObjectIdPath());
        NCypressClient::SetTransactionId(req, UploadTransaction_->GetId());

        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting upload parameters for %v",
            DstObject_.GetPath());
        const auto& rsp = rspOrError.Value();
        ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    void UploadChunks()
    {
        auto proxy = Client_->CreateWriteProxy<TChunkServiceProxy>(DstObject_.ExternalCellTag);

        auto batchReq = proxy->ExecuteBatch();
        NRpc::GenerateMutationId(batchReq);
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
        auto proxy = Client_->CreateWriteProxy<TObjectServiceProxy>(CellTagFromId(DstObject_.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(DstObject_.GetObjectIdPath());
        *req->mutable_statistics() = DataStatistics_;
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

        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error finishing upload to %v",
            DstObject_.GetPath());

        UploadTransaction_->Detach();
    }
};

void TClient::DoConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    TConcatenateNodesOptions options)
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
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TYPathProxy::Exists(path);
    SetTransactionId(req, options, true);
    SetSuppressAccessTracking(req, options);
    SetCachingHeader(req, options);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TYPathProxy::TRspExists>(0)
        .ValueOrThrow();

    return rsp->value();
}

void TClient::DoExternalizeNode(
    const TYPath& path,
    TCellTag cellTag,
    TExternalizeNodeOptions options)
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
    TInternalizeNodeOptions options)
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
    auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
    auto cellTag = PrimaryMasterCellTag;
    switch (type) {
        case EObjectType::TableReplica: {
            {
                auto path = attributes->Get<TString>("table_path");
                InternalValidatePermission(path, EPermission::Write);

                TTableId tableId;
                ResolveExternalTable(path, &tableId, &cellTag);

                attributes->Set("table_path", FromObjectId(tableId));
            }
            {
                auto clusterName = attributes->Get<TString>("cluster_name");
                auto result = WaitFor(NodeExists(GetCypressClusterPath(clusterName), {}));
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking replica cluster existence");
                if (!result.Value()) {
                    THROW_ERROR_EXCEPTION("Replica cluster %Qv does not exist", clusterName);
                }
            }
            break;
        }

        case EObjectType::TabletAction: {
            auto tabletIds = attributes->Get<std::vector<TTabletId>>("tablet_ids");
            if (tabletIds.empty()) {
                THROW_ERROR_EXCEPTION("\"tablet_ids\" are empty");
            }

            cellTag = CellTagFromId(tabletIds[0]);
            break;
        }

        default:
            break;
    }

    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TMasterYPathProxy::CreateObject();
    SetMutationId(req, options);
    req->set_type(static_cast<int>(type));
    req->set_ignore_existing(options.IgnoreExisting);
    if (attributes) {
        ToProto(req->mutable_object_attributes(), *attributes);
    }
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>(0)
        .ValueOrThrow();

    return FromProto<TObjectId>(rsp->object_id());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
