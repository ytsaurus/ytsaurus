#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"

#include <yt/client/object_client/helpers.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/schema_inferer.h>

#include <yt/ytlib/transaction_client/transaction_manager.h>

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

    const TClientPtr Client_;

    const NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    NCypressClient::NProto::TSerializedTree SerializedTree_;

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

        YT_LOG_DEBUG("Requesting serialized tree");

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::BeginCopy(srcPath);
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        SetBeginCopyNodeRequestParameters(req, options);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error requesting serialized tree for %v", srcPath);

        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspBeginCopy>(0);
        const auto& rsp = rspOrError.Value();
        auto opaqueChildIds = FromProto<std::vector<TNodeId>>(rsp->opaque_child_ids());
        auto externalCellTags = FromProto<TCellTagList>(rsp->external_cell_tags());
        SrcNodeId_ = FromProto<TNodeId>(rsp->node_id());

        YT_LOG_DEBUG("Serialized tree received (NodeId: %v, FormatVersion: %v, TreeSize: %v, "
            "OpaqueChildIds: %v, ExternalCellTags: %v)",
            SrcNodeId_,
            rsp->serialized_tree().version(),
            rsp->serialized_tree().data().size(),
            opaqueChildIds,
            externalCellTags);

        SerializedTree_ = std::move(*rsp->mutable_serialized_tree());

        for (auto cellTag : externalCellTags) {
            ExternalCellTags_.push_back(cellTag);
        }

        SortUnique(ExternalCellTags_);
    }

    template <class TOptions>
    void EndCopy(const TYPath& dstPath, const TOptions& options, bool inplace)
    {
        YT_LOG_DEBUG("Materializing serialized trees");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(std::move(channel));

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::EndCopy(dstPath);
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        SetEndCopyNodeRequestParameters(req, options);
        req->set_inplace(inplace);
        *req->mutable_serialized_tree() = std::move(SerializedTree_);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error materializing serialized tree");

        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspEndCopy>(0);
        const auto& rsp = rspOrError.Value();
        DstNodeId_ = FromProto<TNodeId>(rsp->node_id());

        YT_LOG_DEBUG("Serialized trees materialized (NodeId: %v)",
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
        options.TransactionId = Transaction_->GetId();

        auto nodeIdOrError = WaitFor(Client_->CreateNode(
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
            {connection->GetMasterCellId(CellTagFromId(DstNodeId_))},
            connection->GetMasterCellId(CellTag_));

        auto error = WaitFor(future);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing exit cell with entrance cell");

        YT_LOG_DEBUG("Exit cell synchronized with entrance cell");
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
    req->set_force(options.Force);
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

void TClient::DoConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    TConcatenateNodesOptions options)
{
    if (options.Retry) {
        THROW_ERROR_EXCEPTION("\"concatenate\" command is not retriable");
    }

    using NChunkClient::NProto::TDataStatistics;

    std::vector<TString> simpleSrcPaths;
    for (const auto& path : srcPaths) {
        simpleSrcPaths.push_back(path.GetPath());
    }

    const auto& simpleDstPath = dstPath.GetPath();

    bool append = dstPath.GetAppend();

    try {
        std::vector<TUserObject> srcObjects;
        for (const auto& srcPath : srcPaths) {
            srcObjects.emplace_back(srcPath);
        }

        TUserObject dstObject(dstPath);

        std::unique_ptr<IOutputSchemaInferer> outputSchemaInferer;
        std::vector<TSecurityTag> inferredSecurityTags;
        {
            auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
            auto batchReq = proxy->ExecuteBatch();

            for (auto& srcObject : srcObjects) {
                auto req = TObjectYPathProxy::GetBasicAttributes(srcObject.GetPath());
                req->set_populate_security_tags(true);
                req->Tag() = &srcObject;
                SetTransactionId(req, options, true);
                batchReq->AddRequest(req, "get_src_attributes");
            }

            {
                auto req = TObjectYPathProxy::GetBasicAttributes(dstObject.GetPath());
                req->Tag() = &dstObject;
                SetTransactionId(req, options, true);
                batchReq->AddRequest(req, "get_dst_attributes");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting basic attributes of inputs and outputs");
            const auto& batchRsp = batchRspOrError.Value();

            std::optional<EObjectType> commonType;
            std::optional<TString> pathWithCommonType;
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
                        *pathWithCommonType,
                        *commonType);
                }
                commonType = type;
                pathWithCommonType = object.GetPath();
            };

            {
                auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_src_attributes");
                for (const auto& rspOrError : rspsOrError) {
                    const auto& rsp = rspOrError.Value();
                    auto* srcObject = std::any_cast<TUserObject*>(rsp->Tag());

                    srcObject->ObjectId = FromProto<TObjectId>(rsp->object_id());
                    srcObject->ExternalCellTag = rsp->external_cell_tag();
                    srcObject->ExternalTransactionId = rsp->has_external_transaction_id()
                        ? FromProto<TTransactionId>(rsp->external_transaction_id())
                        : options.TransactionId;
                    srcObject->SecurityTags = FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
                    inferredSecurityTags.insert(inferredSecurityTags.end(), srcObject->SecurityTags.begin(), srcObject->SecurityTags.end());

                    YT_LOG_DEBUG("Source table attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v, SecurityTags: %v)",
                        srcObject->GetPath(),
                        srcObject->ObjectId,
                        srcObject->ExternalCellTag,
                        srcObject->SecurityTags);

                    checkType(*srcObject);
                }
            }

            SortUnique(inferredSecurityTags);
            YT_LOG_DEBUG("Security tags inferred (SecurityTags: %v)",
                inferredSecurityTags);

            {
                auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_dst_attributes");
                YT_VERIFY(rspsOrError.size() == 1);
                const auto& rsp = rspsOrError[0].Value();
                auto* dstObject = std::any_cast<TUserObject*>(rsp->Tag());

                dstObject->ObjectId = FromProto<TObjectId>(rsp->object_id());
                dstObject->ExternalCellTag = rsp->external_cell_tag();

                YT_LOG_DEBUG("Destination table attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v)",
                    dstObject->GetPath(),
                    dstObject->ObjectId,
                    dstObject->GetObjectIdPath());

                checkType(*dstObject);
            }

            // Check table schemas.
            if (*commonType == EObjectType::Table) {
                auto createGetSchemaRequest = [&] (const TUserObject& object) {
                    auto req = TYPathProxy::Get(object.GetObjectIdPath() + "/@");
                    req->Tag() = &object;
                    AddCellTagToSyncWith(req, object.ObjectId);
                    SetTransactionId(req, options, false);
                    req->mutable_attributes()->add_keys("schema");
                    req->mutable_attributes()->add_keys("schema_mode");
                    return req;
                };

                TObjectServiceProxy::TRspExecuteBatchPtr getSchemasRsp;
                {
                    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
                    auto getSchemasReq = proxy->ExecuteBatch();
                    {
                        auto req = createGetSchemaRequest(dstObject);
                        getSchemasReq->AddRequest(req, "get_dst_schema");
                    }
                    for (const auto& srcObject : srcObjects) {
                        auto req = createGetSchemaRequest(srcObject);
                        getSchemasReq->AddRequest(req, "get_src_schema");
                    }

                    auto batchRspOrError = WaitFor(getSchemasReq->Invoke());
                    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching table schemas");

                    getSchemasRsp = batchRspOrError.Value();
                }

                {
                    const auto& rspOrErrorList = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_dst_schema");
                    YT_VERIFY(rspOrErrorList.size() == 1);
                    const auto& rsp = rspOrErrorList[0].Value();

                    const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                    const auto schema = attributes->Get<TTableSchema>("schema");
                    const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                    switch (schemaMode) {
                        case ETableSchemaMode::Strong:
                            if (schema.IsSorted()) {
                                THROW_ERROR_EXCEPTION("Destination path %v has sorted schema, concatenation into sorted table is not supported",
                                    dstObject.GetPath());
                            }
                            outputSchemaInferer = CreateSchemaCompatibilityChecker(dstObject.GetPath(), schema);
                            break;
                        case ETableSchemaMode::Weak:
                            outputSchemaInferer = CreateOutputSchemaInferer();
                            if (append) {
                                outputSchemaInferer->AddInputTableSchema(dstObject.GetPath(), schema, schemaMode);
                            }
                            break;
                        default:
                            YT_ABORT();
                    }
                }

                {
                    const auto& rspOrErrors = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_src_schema");
                    YT_VERIFY(rspOrErrors.size() == srcPaths.size());
                    for (const auto& rspOrError : rspOrErrors) {
                        const auto& rsp = rspOrError.Value();
                        auto* srcObject = std::any_cast<const TUserObject*>(rsp->Tag());
                        const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                        const auto schema = attributes->Get<TTableSchema>("schema");
                        const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                        outputSchemaInferer->AddInputTableSchema(srcObject->GetPath(), schema, schemaMode);
                    }
                }
            }
        }

        // Get source chunk ids.
        // Maps src object index -> list of chunk ids for this src.
        std::vector<std::vector<TChunkId>> chunkIdsPerSrc(srcObjects.size());
        {
            THashMap<TCellTag, std::vector<const TUserObject*>> srcExternalCellTagMap;
            for (const auto& srcObject : srcObjects) {
                srcExternalCellTagMap[srcObject.ExternalCellTag].push_back(&srcObject);
            }

            for (const auto& [srcExternalCellTag, thisCellSrcObjects] : srcExternalCellTagMap) {
                auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions(), srcExternalCellTag);
                auto batchReq = proxy->ExecuteBatch();

                for (const auto* srcObject : thisCellSrcObjects) {
                    auto req = TChunkOwnerYPathProxy::Fetch(srcObject->GetObjectIdPath());
                    AddCellTagToSyncWith(req, srcObject->ObjectId);
                    req->Tag() = srcObject;
                    NCypressClient::SetTransactionId(req, srcObject->ExternalTransactionId);
                    ToProto(req->mutable_ranges(), std::vector<TReadRange>{TReadRange()});
                    batchReq->AddRequest(req, "fetch");
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching input chunks");

                const auto& batchRsp = batchRspOrError.Value();
                auto rspsOrError = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspFetch>("fetch");
                for (const auto& rspOrError : rspsOrError) {
                    const auto& rsp = rspOrError.Value();
                    const auto* srcObject = std::any_cast<const TUserObject*>(rsp->Tag());
                    for (const auto& chunk : rsp->chunks()) {
                        auto srcIndex = srcObject - srcObjects.data();
                        chunkIdsPerSrc[srcIndex].push_back(FromProto<TChunkId>(chunk.chunk_id()));
                    }
                }
            }
        }

        // Begin upload.
        TTransactionId uploadTransactionId;
        {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(CellTagFromId(dstObject.ObjectId));

            auto req = TChunkOwnerYPathProxy::BeginUpload(dstObject.GetObjectIdPath());
            req->set_update_mode(static_cast<int>(append ? EUpdateMode::Append : EUpdateMode::Overwrite));
            req->set_lock_mode(static_cast<int>(append ? ELockMode::Shared : ELockMode::Exclusive));
            req->set_upload_transaction_title(Format("Concatenating %v to %v",
                simpleSrcPaths,
                simpleDstPath));
            // NB: Replicate upload transaction to each secondary cell since we have
            // no idea as of where the chunks we're about to attach may come from.
            ToProto(req->mutable_upload_transaction_secondary_cell_tags(), Connection_->GetSecondaryMasterCellTags());
            req->set_upload_transaction_timeout(ToProto<i64>(Connection_->GetConfig()->UploadTransactionTimeout));
            NRpc::GenerateMutationId(req);
            SetTransactionId(req, options, true);

            auto rspOrError = WaitFor(proxy->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting upload to %v",
                simpleDstPath);
            const auto& rsp = rspOrError.Value();

            uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        }

        auto uploadTransaction = TransactionManager_->Attach(uploadTransactionId, TTransactionAttachOptions{
            .AutoAbort = true,
            .PingAncestors = options.PingAncestors
        });

        // Flatten chunk ids.
        std::vector<TChunkId> flatChunkIds;
        for (const auto& chunkIds : chunkIdsPerSrc) {
            flatChunkIds.insert(flatChunkIds.end(), chunkIds.begin(), chunkIds.end());
        }

        // Teleport chunks.
        {
            auto teleporter = New<TChunkTeleporter>(
                Connection_->GetConfig(),
                this,
                Connection_->GetInvoker(),
                uploadTransactionId,
                Logger);

            for (auto chunkId : flatChunkIds) {
                teleporter->RegisterChunk(chunkId, dstObject.ExternalCellTag);
            }

            WaitFor(teleporter->Run())
                .ThrowOnError();
        }

        // Get upload params.
        TChunkListId chunkListId;
        {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(dstObject.ExternalCellTag);

            auto req = TChunkOwnerYPathProxy::GetUploadParams(dstObject.GetObjectIdPath());
            NCypressClient::SetTransactionId(req, uploadTransactionId);

            auto rspOrError = WaitFor(proxy->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting upload parameters for %v",
                simpleDstPath);
            const auto& rsp = rspOrError.Value();

            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }

        // Attach chunks to chunk list.
        TDataStatistics dataStatistics;
        {
            auto proxy = CreateWriteProxy<TChunkServiceProxy>(dstObject.ExternalCellTag);

            auto batchReq = proxy->ExecuteBatch();
            NRpc::GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            auto req = batchReq->add_attach_chunk_trees_subrequests();
            ToProto(req->mutable_parent_id(), chunkListId);
            ToProto(req->mutable_child_ids(), flatChunkIds);
            req->set_request_statistics(true);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to %v",
                simpleDstPath);
            const auto& batchRsp = batchRspOrError.Value();

            const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);
            dataStatistics = rsp.statistics();
        }

        // End upload.
        {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(CellTagFromId(dstObject.ObjectId));

            auto req = TChunkOwnerYPathProxy::EndUpload(dstObject.GetObjectIdPath());
            *req->mutable_statistics() = dataStatistics;
            if (outputSchemaInferer) {
                ToProto(req->mutable_table_schema(), outputSchemaInferer->GetOutputTableSchema());
                req->set_schema_mode(static_cast<int>(outputSchemaInferer->GetOutputTableSchemaMode()));
            }

            std::vector<TSecurityTag> securityTags;
            if (auto explicitSecurityTags = dstPath.GetSecurityTags()) {
                // TODO(babenko): audit
                YT_LOG_INFO("Destination table is assigned explicit security tags (Path: %v, InferredSecurityTags: %v, ExplicitSecurityTags: %v)",
                    simpleDstPath,
                    inferredSecurityTags,
                    explicitSecurityTags);
                securityTags = *explicitSecurityTags;
            } else {
                YT_LOG_INFO("Destination table is assigned automatically-inferred security tags (Path: %v, SecurityTags: %v)",
                    simpleDstPath,
                    inferredSecurityTags);
                securityTags = inferredSecurityTags;
            }

            ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
            NCypressClient::SetTransactionId(req, uploadTransactionId);
            NRpc::GenerateMutationId(req);

            auto rspOrError = WaitFor(proxy->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error finishing upload to %v",
                simpleDstPath);
        }

        uploadTransaction->Detach();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error concatenating %v to %v",
            simpleSrcPaths,
            simpleDstPath)
            << ex;
    }
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

TObjectId TClient::DoCreateObject(
    EObjectType type,
    const TCreateObjectOptions& options)
{
    auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
    auto cellTag = PrimaryMasterCellTag;
    switch (type) {
        case EObjectType::TableReplica: {
            auto path = attributes->Get<TString>("table_path");
            InternalValidatePermission(path, EPermission::Write);

            TTableId tableId;
            ResolveExternalTable(path, &tableId, &cellTag);

            attributes->Set("table_path", FromObjectId(tableId));
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
