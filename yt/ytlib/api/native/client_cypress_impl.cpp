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

#include <yt/ytlib/hive/helpers.h>

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
    req->set_preserve_creation_time(options.PreserveCreationTime);
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
    req->set_remove_source(false);
}

void SetCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_remove_source(true);
}

void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TCopyNodeOptions& /*options*/)
{
    req->set_remove_source(false);
}

void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TMoveNodeOptions& /*options*/)
{
    req->set_remove_source(true);
}

void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TCopyNodeOptions& options)
{
    SetCopyNodeBaseRequestParameters(req, options);
    req->set_remove_source(false);
}

void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_remove_source(true);
}

template <class TOptions>
class TCrossCellNodeCopier
{
public:
    TCrossCellNodeCopier(
        TClientPtr client,
        TYPath srcPath,
        TYPath dstPath,
        const TOptions& options,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , SrcPath_(std::move(srcPath))
        , DstPath_(std::move(dstPath))
        , Options_(options)
        , Logger(logger
            .AddTag("SrcPath: %v, DstPath: %v",
                SrcPath_,
                DstPath_))
    { }

    TNodeId Run()
    {
        YT_LOG_DEBUG("Cross-cell node copying started");
        StartTransaction();
        BeginCopy();
        EndCopy();
        SyncCells();
        CommitTransaction();
        YT_LOG_DEBUG("Cross-cell node copying completed");
        return NodeId_;
    }

private:
    const TClientPtr Client_;
    const TYPath SrcPath_;
    const TYPath DstPath_;
    const TOptions Options_;

    const NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    NCypressClient::NProto::TSerializedTree SerializedTree_;

    TNodeId NodeId_;
    std::vector<TCellTag> ExternalCellTags_;

    void StartTransaction()
    {
        YT_LOG_DEBUG("Starting copy transaction");

        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", Format("Copy %v to %v", SrcPath_, DstPath_));

        auto transactionOrError = WaitFor(Client_->StartNativeTransaction(
            ETransactionType::Master,
            TTransactionStartOptions{
                .ParentId = Options_.TransactionId,
                .Attributes = std::move(transactionAttributes)
            }));
        THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting copy transaction");
        Transaction_ = transactionOrError.Value();

        YT_LOG_DEBUG("Copy transaction started (TransactionId: %v)",
            Transaction_->GetId());
    }

    void BeginCopy()
    {
        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(std::move(channel));

        YT_LOG_DEBUG("Requesting serialized tree");

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::BeginCopy(SrcPath_);
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        SetBeginCopyNodeRequestParameters(req, Options_);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error requesting serialized tree for %v", SrcPath_);
        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspBeginCopy>(0);
        const auto& rsp = rspOrError.Value();
        auto opaqueChildIds = FromProto<std::vector<TNodeId>>(rsp->opaque_child_ids());
        auto externalCellTags = FromProto<TCellTagList>(rsp->external_cell_tags());

        YT_LOG_DEBUG("Serialized tree received (FormatVersion: %v, TreeSize: %v, "
            "OpaqueChildIds: %v, ExternalCellTags: %v)",
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

    void EndCopy()
    {
        YT_LOG_DEBUG("Materializing serialized trees");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(std::move(channel));

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::EndCopy(DstPath_);
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        SetEndCopyNodeRequestParameters(req, Options_);
        *req->mutable_serialized_tree() = std::move(SerializedTree_);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error materializing serialized tree");

        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspEndCopy>(0);
        const auto& rsp = rspOrError.Value();
        NodeId_ = FromProto<TNodeId>(rsp->node_id());

        YT_LOG_DEBUG("Serialized trees materialized (NodeId: %v)",
            NodeId_);
    }

    void SyncCells()
    {
        if (ExternalCellTags_.empty()) {
            return;
        }

        YT_LOG_DEBUG("Synchronizing cells");

        auto nodeCellTag = CellTagFromId(NodeId_);
        const auto& connection = Client_->GetNativeConnection();
        std::vector<TFuture<void>> futures;
        for (auto externalCellTag : ExternalCellTags_) {
            futures.push_back(SyncHiveCellWithOthers(
                connection->GetCellDirectory(),
                {connection->GetMasterCellId(nodeCellTag)},
                connection->GetMasterCellId(externalCellTag),
                connection->GetConfig()->HiveSyncRpcTimeout));
        }

        auto error = WaitFor(Combine(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing external nodes cells with the cloned node cell");

        YT_LOG_DEBUG("Cells synchronized");
    }

    void CommitTransaction()
    {
        YT_LOG_DEBUG("Committing copy transaction");

        auto error = WaitFor(Transaction_->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error committing copy transaction");

        YT_LOG_DEBUG("Copy transaction committed");
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

    TCrossCellNodeCopier<TOptions> copier(
        this,
        srcPath,
        dstPath,
        options,
        Logger);
    return copier.Run();
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
        // Get objects ids.
        std::vector<TObjectId> srcIds;
        TCellTagList srcCellTags;
        TObjectId dstId;
        TCellTag dstNativeCellTag;
        TCellTag dstExternalCellTag;
        std::unique_ptr<IOutputSchemaInferer> outputSchemaInferer;
        std::vector<TSecurityTag> inferredSecurityTags;
        {
            auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
            auto batchReq = proxy->ExecuteBatch();

            for (const auto& path : srcPaths) {
                auto req = TObjectYPathProxy::GetBasicAttributes(path.GetPath());
                req->set_populate_security_tags(true);
                SetTransactionId(req, options, true);
                batchReq->AddRequest(req, "get_src_attributes");
            }

            {
                auto req = TObjectYPathProxy::GetBasicAttributes(simpleDstPath);
                SetTransactionId(req, options, true);
                batchReq->AddRequest(req, "get_dst_attributes");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of inputs and outputs");
            const auto& batchRsp = batchRspOrError.Value();

            std::optional<EObjectType> commonType;
            std::optional<TString> pathWithCommonType;
            auto checkType = [&] (EObjectType type, const TYPath& path) {
                if (type != EObjectType::Table && type != EObjectType::File) {
                    THROW_ERROR_EXCEPTION("Type of %v must be either %Qlv or %Qlv",
                        path,
                        EObjectType::Table,
                        EObjectType::File);
                }
                if (commonType && *commonType != type) {
                    THROW_ERROR_EXCEPTION("Type of %v (%Qlv) must be the same as type of %v (%Qlv)",
                        path,
                        type,
                        *pathWithCommonType,
                        *commonType);
                }
                commonType = type;
                pathWithCommonType = path;
            };

            {
                auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_src_attributes");
                for (int srcIndex = 0; srcIndex < srcPaths.size(); ++srcIndex) {
                    const auto& srcPath = srcPaths[srcIndex];
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspsOrError[srcIndex], "Error getting attributes of %v",
                        srcPath.GetPath());
                    const auto& rsp = rspsOrError[srcIndex].Value();

                    auto id = FromProto<TObjectId>(rsp->object_id());
                    srcIds.push_back(id);

                    auto externalCellTag = rsp->external_cell_tag();
                    srcCellTags.push_back(externalCellTag);

                    auto securityTags = FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
                    inferredSecurityTags.insert(inferredSecurityTags.end(), securityTags.begin(), securityTags.end());

                    YT_LOG_DEBUG("Source table attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v, SecurityTags: %v)",
                        srcPath.GetPath(),
                        id,
                        externalCellTag,
                        securityTags);

                    checkType(TypeFromId(id), srcPath.GetPath());
                }
            }

            SortUnique(inferredSecurityTags);
            YT_LOG_DEBUG("Security tags inferred (SecurityTags: %v)",
                inferredSecurityTags);

            {
                auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_dst_attributes");
                THROW_ERROR_EXCEPTION_IF_FAILED(rspsOrError[0], "Error getting attributes of %v",
                    simpleDstPath);
                const auto& rsp = rspsOrError[0].Value();

                dstId = FromProto<TObjectId>(rsp->object_id());
                dstNativeCellTag = CellTagFromId(dstId);

                YT_LOG_DEBUG("Destination table attributes received (Path: %v, ObjectId: %v, ExternalCellTag: %v)",
                    simpleDstPath,
                    dstId,
                    dstExternalCellTag);

                checkType(TypeFromId(dstId), simpleDstPath);
            }

            // Check table schemas.
            if (*commonType == EObjectType::Table) {
                auto createGetSchemaRequest = [&] (TObjectId objectId) {
                    auto req = TYPathProxy::Get(FromObjectId(objectId) + "/@");
                    SetTransactionId(req, options, true);
                    req->mutable_attributes()->add_keys("schema");
                    req->mutable_attributes()->add_keys("schema_mode");
                    return req;
                };

                TObjectServiceProxy::TRspExecuteBatchPtr getSchemasRsp;
                {
                    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
                    auto getSchemasReq = proxy->ExecuteBatch();
                    {
                        auto req = createGetSchemaRequest(dstId);
                        getSchemasReq->AddRequest(req, "get_dst_schema");
                    }
                    for (auto id : srcIds) {
                        auto req = createGetSchemaRequest(id);
                        getSchemasReq->AddRequest(req, "get_src_schema");
                    }

                    auto batchResponseOrError = WaitFor(getSchemasReq->Invoke());
                    THROW_ERROR_EXCEPTION_IF_FAILED(batchResponseOrError, "Error fetching table schemas");

                    getSchemasRsp = batchResponseOrError.Value();
                }

                {
                    const auto& rspOrErrorList = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_dst_schema");
                    YT_VERIFY(rspOrErrorList.size() == 1);
                    const auto& rspOrError = rspOrErrorList[0];
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching schema for %v",
                        simpleDstPath);

                    const auto& rsp = rspOrError.Value();
                    const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                    const auto schema = attributes->Get<TTableSchema>("schema");
                    const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                    switch (schemaMode) {
                        case ETableSchemaMode::Strong:
                            if (schema.IsSorted()) {
                                THROW_ERROR_EXCEPTION("Destination path %v has sorted schema, concatenation into sorted table is not supported",
                                    simpleDstPath);
                            }
                            outputSchemaInferer = CreateSchemaCompatibilityChecker(simpleDstPath, schema);
                            break;
                        case ETableSchemaMode::Weak:
                            outputSchemaInferer = CreateOutputSchemaInferer();
                            if (append) {
                                outputSchemaInferer->AddInputTableSchema(simpleDstPath, schema, schemaMode);
                            }
                            break;
                        default:
                            YT_ABORT();
                    }
                }

                {
                    const auto& rspOrErrorList = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_src_schema");
                    YT_VERIFY(rspOrErrorList.size() == srcPaths.size());
                    for (size_t i = 0; i < rspOrErrorList.size(); ++i) {
                        const auto& path = srcPaths[i];
                        const auto& rspOrError = rspOrErrorList[i];
                        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching schema for %v",
                            path.GetPath());

                        const auto& rsp = rspOrError.Value();
                        const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                        const auto schema = attributes->Get<TTableSchema>("schema");
                        const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                        outputSchemaInferer->AddInputTableSchema(path.GetPath(), schema, schemaMode);
                    }
                }
            }
        }

        // Get source chunk ids.
        // Maps src index -> list of chunk ids for this src.
        std::vector<std::vector<TChunkId>> groupedChunkIds(srcPaths.size());
        {
            THashMap<TCellTag, std::vector<int>> cellTagToIndexes;
            for (int srcIndex = 0; srcIndex < srcCellTags.size(); ++srcIndex) {
                cellTagToIndexes[srcCellTags[srcIndex]].push_back(srcIndex);
            }

            for (const auto& [srcCellTag, srcIndexes] : cellTagToIndexes) {
                auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions(), srcCellTag);
                auto batchReq = proxy->ExecuteBatch();

                for (int localIndex = 0; localIndex < srcIndexes.size(); ++localIndex) {
                    int srcIndex = srcIndexes[localIndex];
                    auto srcId = srcIds[srcIndex];
                    auto req = TChunkOwnerYPathProxy::Fetch(FromObjectId(srcId));
                    AddCellTagToSyncWith(req, srcId);
                    SetTransactionId(req, options, true);
                    ToProto(req->mutable_ranges(), std::vector<TReadRange>{TReadRange()});
                    batchReq->AddRequest(req, "fetch");
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error fetching inputs");

                const auto& batchRsp = batchRspOrError.Value();
                auto rspsOrError = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspFetch>("fetch");
                for (int localIndex = 0; localIndex < srcIndexes.size(); ++localIndex) {
                    int srcIndex = srcIndexes[localIndex];
                    const auto& rspOrError = rspsOrError[localIndex];
                    const auto& path = srcPaths[srcIndex];
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching %v",
                        path.GetPath());
                    const auto& rsp = rspOrError.Value();

                    for (const auto& chunk : rsp->chunks()) {
                        groupedChunkIds[srcIndex].push_back(FromProto<TChunkId>(chunk.chunk_id()));
                    }
                }
            }
        }

        // Begin upload.
        TTransactionId uploadTransactionId;
        const auto dstIdPath = FromObjectId(dstId);
        {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(dstNativeCellTag);

            auto req = TChunkOwnerYPathProxy::BeginUpload(dstIdPath);
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
            dstExternalCellTag = rsp->cell_tag();
        }

        auto uploadTransaction = TransactionManager_->Attach(uploadTransactionId, TTransactionAttachOptions{
            .AutoAbort = true,
            .PingAncestors = options.PingAncestors
        });

        // Flatten chunk ids.
        std::vector<TChunkId> flatChunkIds;
        for (const auto& ids : groupedChunkIds) {
            flatChunkIds.insert(flatChunkIds.end(), ids.begin(), ids.end());
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
                teleporter->RegisterChunk(chunkId, dstExternalCellTag);
            }

            WaitFor(teleporter->Run())
                .ThrowOnError();
        }

        // Get upload params.
        TChunkListId chunkListId;
        {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(dstExternalCellTag);

            auto req = TChunkOwnerYPathProxy::GetUploadParams(dstIdPath);
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
            auto proxy = CreateWriteProxy<TChunkServiceProxy>(dstExternalCellTag);

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
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(dstNativeCellTag);

            auto req = TChunkOwnerYPathProxy::EndUpload(dstIdPath);
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
