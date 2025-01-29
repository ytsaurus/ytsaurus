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
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
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
#include <yt/yt/core/ypath/tokenizer.h>

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
    req->set_mode(ToProto(ENodeCloneMode::Copy));
    req->set_allow_secondary_index_abandonment(options.AllowSecondaryIndexAbandonment);
}

void SetCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_mode(ToProto(ENodeCloneMode::Move));
    req->set_allow_secondary_index_abandonment(options.AllowSecondaryIndexAbandonment);
}

// COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TCopyNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Copy));
}

// COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
void SetBeginCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqBeginCopyPtr& req,
    const TMoveNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Move));
}

void SetLockCopySourceRequestParameters(
    const TCypressYPathProxy::TReqLockCopySourcePtr& req,
    const TCopyNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Copy));
}

void SetLockCopySourceRequestParameters(
    const TCypressYPathProxy::TReqLockCopySourcePtr& req,
    const TMoveNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Move));
}

// COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TCopyNodeOptions& options)
{
    SetCopyNodeBaseRequestParameters(req, options);
    req->set_mode(ToProto(ENodeCloneMode::Copy));
}

// COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
void SetEndCopyNodeRequestParameters(
    const TCypressYPathProxy::TReqEndCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetMoveNodeBaseRequestParameters(req, options);
    req->set_mode(ToProto(ENodeCloneMode::Move));
}

void SetLockCopyDestinationRequestParametersBase(
    const TCypressYPathProxy::TReqLockCopyDestinationPtr& req,
    const TCopyNodeOptionsBase& options)
{
    req->set_preserve_acl(options.PreserveAcl);
    req->set_force(options.Force);
}

void SetLockCopyDestinationRequestParameters(
    const TCypressYPathProxy::TReqLockCopyDestinationPtr& req,
    const TCopyNodeOptions& options)
{
    SetLockCopyDestinationRequestParametersBase(req, options);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
}

void SetLockCopyDestinationRequestParameters(
    const TCypressYPathProxy::TReqLockCopyDestinationPtr& req,
    const TMoveNodeOptions& options)
{
    SetLockCopyDestinationRequestParametersBase(req, options);
}

void SetSerializeNodeRequestParameters(
    const TCypressYPathProxy::TReqSerializeNodePtr& req,
    const TCopyNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Copy));
}

void SetSerializeNodeRequestParameters(
    const TCypressYPathProxy::TReqSerializeNodePtr& req,
    const TMoveNodeOptions& /*options*/)
{
    req->set_mode(ToProto(ENodeCloneMode::Move));
}

void SetAssembleTreeCopyRequestParametersCore(
    const TCypressYPathProxy::TReqAssembleTreeCopyPtr& req,
    const TCopyNodeOptionsBase& options)
{
    req->set_preserve_modification_time(options.PreserveModificationTime);
    req->set_preserve_acl(options.PreserveAcl);
    req->set_force(options.Force);
    req->set_recursive(options.Recursive);
    req->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
}

void SetAssembleTreeCopyRequestParameters(
    const TCypressYPathProxy::TReqAssembleTreeCopyPtr& req,
    const TCopyNodeOptions& options)
{
    SetAssembleTreeCopyRequestParametersCore(req, options);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
}

void SetAssembleTreeCopyRequestParameters(
    const TCypressYPathProxy::TReqAssembleTreeCopyPtr& req,
    const TMoveNodeOptions& options)
{
    SetAssembleTreeCopyRequestParametersCore(req, options);
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

    // COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
    struct TSerializedSubtree
    {
        TSerializedSubtree() = default;

        TSerializedSubtree(
            TYPath path,
            int version,
            NProtoBuf::RepeatedPtrField<NCypressClient::NProto::TSerializedNode> serializedNodes,
            NProtoBuf::RepeatedPtrField<NYT::NProto::TGuid> schemaIds)
            : Path(std::move(path))
            , Version(version)
            , SerializedNodes(std::move(serializedNodes))
            , SchemaIds(std::move(schemaIds))
        { }

        // COMPAT(h0pless): RefactorCrossCellCopyInPreparationForSequoia.
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
        int Version;

        //! Serialized subtree.
        NProtoBuf::RepeatedPtrField<NCypressClient::NProto::TSerializedNode> SerializedNodes;

        //! Schema IDs referenced in #SerializedNodes.
        NProtoBuf::RepeatedPtrField<NYT::NProto::TGuid> SchemaIds;

        //! Serialized subtree.
        // COMPAT(h0pless): RefactorCrossCellCopyInPreparationForSequoia.
        NCypressClient::NProto::TSerializedTree SerializedValue;
        //! These schemas are referenced from #SerializedValue (by serialization keys).
        // COMPAT(h0pless): RefactorCrossCellCopyInPreparationForSequoia.
        NProtoBuf::RepeatedPtrField<NCypressClient::NProto::TRegisteredSchema> Schemas;
    };

    const TClientPtr Client_;

    const NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    bool TransactionCommitted_ = false;

    std::vector<TSerializedSubtree> SerializedSubtrees_;

    TNodeId SrcNodeId_;
    TYPath ResolvedSrcNodePath_;
    TNodeId DstNodeId_ = NullObjectId;
    TCellTag DstCellTag_ = InvalidCellTag;

    THashSet<TCellTag> ExternalCellTags_;

    THashMap<TMasterTableSchemaId, TTableSchema> SchemaIdToSchema_;
    THashMap<TMasterTableSchemaId, TMasterTableSchemaId> SrcToDstSchemaIdMapping_;
    THashSet<TMasterTableSchemaId> SchemaIds_;

    THashMap<TNodeId, TCypressYPathProxy::TRspSerializeNodePtr> NodeIdToSerializedData_;
    THashMap<TNodeId, TNodeId> SrcToDstNodeIdMapping_;

    IAttributeDictionaryPtr DstInheritableAttributes_;
    THashMap<TNodeId, IAttributeDictionaryPtr> NodeToInheritedAttributesOverride_;

    TAccountId NewAccountId_;

    struct TNodeIdToChildren
    {
        struct TChildInfo
        {
            TString Key;
            TNodeId Id;
        };

        TNodeId NodeId;
        std::vector<TChildInfo> Children;
    };
    std::vector<TNodeIdToChildren> NodeIdToChildrenMapping_;

    int ProtocolVersion_ = -1;

    // COMPAT(h0pless): RefactorCrossCellCopyInPreparationForSequoia.
    bool UseNewerCrossCellCopyProtocol_ = false;


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
    [[nodiscard]] bool BeginCopy(const TYPath& srcPath, const TOptions& options, bool allowRootLink)
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
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                for (const auto& innerError : cumulativeError.InnerErrors()) {
                    THROW_ERROR_EXCEPTION_IF(
                        innerError.GetCode() != NObjectClient::EErrorCode::BeginCopyDeprecated,
                        TError("Error requesting serialized subtree for %v", subtreePath)
                            << cumulativeError);
                }
                return false;
            }

            const auto& batchRsp = batchRspOrError.Value();

            auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspBeginCopy>(0);
            const auto& rsp = rspOrError.Value();
            auto portalChildIds = FromProto<std::vector<TNodeId>>(rsp->portal_child_ids());
            auto externalCellTags = FromProto<TCellTagList>(rsp->external_cell_tags());
            auto opaqueChildPaths = FromProto<std::vector<TYPath>>(rsp->opaque_child_paths());

            if (rsp->has_version()) {
                // EMasterReign::RefactorCrossCellCopyInPreparationForSequoia.
                YT_VERIFY(rsp->version() >= 2726);
                UseNewerCrossCellCopyProtocol_ = true;
            }

            auto subtreeRootId = UseNewerCrossCellCopyProtocol_
                ? FromProto<TNodeId>(rsp->serialized_nodes()[0].node_id())
                : FromProto<TNodeId>(rsp->node_id()); // root node id

            if (!allowRootLink && subtreePath == srcPath) {
                SrcNodeId_ = subtreeRootId;
            }

            size_t dataSize = 0;
            if (UseNewerCrossCellCopyProtocol_) {
                for (const auto& serializedNode : rsp->serialized_nodes()) {
                    dataSize += serializedNode.data().size();
                }
            } else {
                dataSize = rsp->serialized_tree().data().size();
            }
            YT_LOG_DEBUG("Serialized subtree received (RootNodeId: %v, Path: %v, FormatVersion: %v, TreeSize: %v, "
                "PortalChildIds: %v, ExternalCellTags: %v, OpaqueChildPaths: %v, RegisteredSchemaCount: %v)",
                subtreeRootId,
                subtreePath,
                UseNewerCrossCellCopyProtocol_ ? rsp->version() : rsp->serialized_tree().version(),
                dataSize,
                portalChildIds,
                externalCellTags,
                opaqueChildPaths,
                UseNewerCrossCellCopyProtocol_ ? rsp->schema_ids_size() : rsp->schemas_size()); // update value

            auto relativePath = TryComputeYPathSuffix(subtreePath, ResolvedSrcNodePath_);
            YT_VERIFY(relativePath);

            if (UseNewerCrossCellCopyProtocol_) {
                SchemaIds_ = FromProto<THashSet<TMasterTableSchemaId>>(rsp->schema_ids());

                SerializedSubtrees_.emplace_back(
                    *relativePath,
                    rsp->version(),
                    std::move(*rsp->mutable_serialized_nodes()),
                    std::move(*rsp->mutable_schema_ids()));
            } else {
                SerializedSubtrees_.emplace_back(
                    *relativePath,
                    std::move(*rsp->mutable_serialized_tree()),
                    std::move(*rsp->mutable_schemas()));
            }

            for (auto cellTag : externalCellTags) {
                ExternalCellTags_.emplace(cellTag);
            }

            for (const auto& opaqueChildPath : opaqueChildPaths) {
                subtreeSerializationQueue.push(opaqueChildPath);
            }
        }

        FetchMasterTableSchemas();
        return true;
    }

    // Returns a flag signaling if copy should continue.
    // In essence returns false iff "ignore_existing" option was set and node exists.
    template <class TOptions>
    [[nodiscard]] bool LockCopyDestination(const TYPath& dstPath, const TOptions& options)
    {
        YT_LOG_DEBUG("Locking destination (DestinationPath: %v)",
            dstPath);

        auto proxy = CreateObjectServiceWriteProxy(Client_);

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::LockCopyDestination(dstPath);
        SetTransactionId(req, Transaction_->GetId());
        SetLockCopyDestinationRequestParameters(req, options);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error locking destination node");
        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspLockCopyDestination>(0);
        const auto& rsp = rspOrError.Value();
        if (rsp->has_existing_node_id()) {
            DstNodeId_ = FromProto<TNodeId>(rsp->existing_node_id());
            YT_LOG_DEBUG("Destination node already exists (NodeId: %v)",
                DstNodeId_);
            return false;
        }

        DstInheritableAttributes_ = ConvertToAttributes(rsp->effective_inheritable_attributes());
        if (DstCellTag_ == InvalidCellTag) {
            DstCellTag_ = FromProto<TCellTag>(rsp->native_cell_tag());
        }

        if (options.PreserveAccount) {
            YT_VERIFY(!NewAccountId_);
        } else {
            NewAccountId_ = FromProto<TAccountId>(rsp->account_id());
        }

        YT_LOG_DEBUG("Locked destination node (DstCellTag: %v, AccountId: %v, EffectiveInheritableAttributes: %v)",
            DstCellTag_,
            NewAccountId_,
            DstInheritableAttributes_->ListPairs());

        return true;
    }

    template <class TOptions>
    void LockCopySource(const TOptions& options, bool allowRootLink)
    {
        // TODO(h0pless): Here we rely on BeginCopy to call ResolveSourceNode if needed.
        // Once BeginCopy will be deleted, make sure that ResolvedSrcNodePath_ is set correctly.
        auto proxy = CreateObjectServiceWriteProxy(Client_);

        YT_LOG_DEBUG("Locking source tree (Path: %v)", ResolvedSrcNodePath_);

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::LockCopySource(ResolvedSrcNodePath_);
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        SetLockCopySourceRequestParameters(req, options);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Error locking source tree for %v", ResolvedSrcNodePath_);

        const auto& batchRsp = batchRspOrError.Value();

        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspLockCopySource>(0);
        const auto& rsp = rspOrError.Value();

        auto subtreeRootId = FromProto<TNodeId>(rsp->root_node_id());

        if (!allowRootLink) {
            // This looks bizzare, but it has to be written because:
            // 1. Externalize call uses the same API as the "copy" command;
            // 2. It's ok to call cross-cell copy command on symlinks;
            // 3. It's not ok to attempt to externalize a symlink;
            // 4. ResolveSourceNode sets a correct value to SrcNodeId_ and to ResolvedSrcNodePath_;
            // 5. Because of (3) ResolveSourceNode is not being called for externalize, and ResolvedSrcNodePath_
            // gets set to the path, specified by the user.
            // Now we have ID of the source node, it's time to set it.
            // TODO(h0pless): When refactoring this file, think about doing some check on master-side.
            // It will allow to throw an error from there and remove this weird if statement here.
            SrcNodeId_ = subtreeRootId;
        } else {
            YT_VERIFY(SrcNodeId_ == subtreeRootId);
        }

        NodeIdToChildrenMapping_.reserve(rsp->node_id_to_children_size());
        for (const auto& entry : rsp->node_id_to_children()) {
            auto& nodeIdToChildren = NodeIdToChildrenMapping_.emplace_back();
            nodeIdToChildren.NodeId = FromProto<TNodeId>(entry.node_id());
            nodeIdToChildren.Children.reserve(entry.children_size());
            for (const auto& child : entry.children()) {
                nodeIdToChildren.Children.push_back({
                    .Key = child.key(),
                    .Id = FromProto<TNodeId>(child.id())});
            }
        }

        // If src node cannot have children, then push it into the container.
        if (NodeIdToChildrenMapping_.empty()) {
            NodeIdToChildrenMapping_.push_back({
                .NodeId = SrcNodeId_,
                .Children = {}});
        }

        ProtocolVersion_ = rsp->version();

        YT_LOG_DEBUG("Source tree locked (RootNodeId: %v, Path: %v, NodeIdToChildrenMappingSize: %v)",
            subtreeRootId,
            ResolvedSrcNodePath_,
            NodeIdToChildrenMapping_.size());
    }

    template <class TOptions>
    void GetSerializedNodes(const TOptions& options)
    {
        std::vector<TNodeId> nodesToFetch;
        // Every node except root node and leaf nodes is mentioned twice.
        // This adds every node exactly once.
        nodesToFetch.push_back(NodeIdToChildrenMapping_[0].NodeId);
        for (const auto& nodeMetadata : NodeIdToChildrenMapping_) {
            for (auto child : nodeMetadata.Children) {
                nodesToFetch.push_back(child.Id);
            }
        }

        YT_LOG_DEBUG("Getting serialized nodes (NodeCount: %v)",
            nodesToFetch.size());

        auto requestTemplate = TCypressYPathProxy::SerializeNode();
        SetSerializeNodeRequestParameters(requestTemplate, options);

        auto vectorizedBatcher = TMasterYPathProxy::CreateSerializeNodeBatcher(
            Client_,
            requestTemplate,
            nodesToFetch,
            Transaction_->GetId());

        auto nodeIdToRspOrError = WaitFor(vectorizedBatcher.Invoke())
            .ValueOrThrow();
        for (const auto& [nodeId, rspOrError] : nodeIdToRspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting serialized nodes from master");
            EmplaceOrCrash(NodeIdToSerializedData_, nodeId, rspOrError.Value());
        }

        // COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy. Sanity check.
        // Safe to remove when this protocol is tested.
        YT_ASSERT(SchemaIds_.empty());
        YT_ASSERT(ExternalCellTags_.empty());

        for (const auto& [_, data] : NodeIdToSerializedData_) {
            if (auto schemaId = YT_PROTO_OPTIONAL(data->serialized_node(), schema_id, TMasterTableSchemaId)) {
                SchemaIds_.emplace(*schemaId);
            }

            if (auto cellTag = YT_PROTO_OPTIONAL(data->serialized_node(), external_cell_tag, TCellTag)) {
                ExternalCellTags_.emplace(*cellTag);
            }
        }

        auto dataSize = std::accumulate(
            NodeIdToSerializedData_.begin(),
            NodeIdToSerializedData_.end(),
            i64(0),
            [] (i64 size, const std::pair<TNodeId, TCypressYPathProxy::TRspSerializeNodePtr>& value) {
                return size + std::ssize(value.second->serialized_node().data());
            });

        YT_LOG_DEBUG("Finished getting serialized nodes (NodeCount: %v, DataSize: %v)",
            NodeIdToSerializedData_.size(),
            dataSize);

        FetchMasterTableSchemas();
    }

    void FetchMasterTableSchemas()
    {
        if (SchemaIds_.empty()) {
            return;
        }

        YT_LOG_DEBUG("Fetching table schemas used in the subtree");

        auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower);

        auto batchReq = proxy.ExecuteBatch();
        for (const auto& schemaId : SchemaIds_) {
            auto serializedId = FromObjectId(schemaId);
            auto req = TYPathProxy::Get(serializedId);
            SetTransactionId(req, Transaction_->GetId());
            batchReq->AddRequest(req, serializedId);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Error fetching table schemas");

        const auto& batchRsp = batchRspOrError.Value();

        for (const auto& schemaId: SchemaIds_) {
            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(FromObjectId(schemaId));
            auto rsp = rspOrError.Value();
            auto schemaYson = TYsonString(rsp->value());

            EmplaceOrCrash(SchemaIdToSchema_, schemaId, ConvertTo<TTableSchema>(schemaYson));
        }

        YT_LOG_DEBUG("Finished fetching schemas (SchemaCount: %v)",
            SchemaIdToSchema_.size());
    }

    void CalculateInheritedAttributes(const TYPath& srcPath)
    {
        YT_LOG_DEBUG("Calculating attributes inherited during copy");

        auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower);

        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::CalculateInheritedAttributes(srcPath);

        ToProto(req->mutable_dst_attributes(), *DstInheritableAttributes_);
        SetTransactionId(req, Transaction_->GetId());
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Error calculating inherited attributes for %v", srcPath);

        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspCalculateInheritedAttributes>(0);
        const auto& rsp = rspOrError.Value();
        for (const auto& entry : rsp->node_to_attribute_deltas()) {
            auto nodeId = FromProto<TNodeId>(entry.node_id());
            EmplaceOrCrash(NodeToInheritedAttributesOverride_, nodeId, FromProto(entry.attributes()));
        }

        YT_LOG_DEBUG("Finished calculating attributes inherited during copy (OverrideCount: %v)",
            NodeToInheritedAttributesOverride_.size());
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

            if (UseNewerCrossCellCopyProtocol_) {
                *req->mutable_serialized_nodes() = std::move(subtree.SerializedNodes);
                req->set_version(subtree.Version);

                req->mutable_schema_id_to_schema()->Reserve(SchemaIdToSchema_.size());
                for (const auto& [schemaId, schema] : SchemaIdToSchema_) {
                    auto* schemaIdToSchemaEntry = req->add_schema_id_to_schema();
                    ToProto(schemaIdToSchemaEntry->mutable_schema_id(), schemaId);
                    ToProto(schemaIdToSchemaEntry->mutable_schema(), schema);
                }
            } else {
                // COMPAT(h0pless): RefactorCrossCellCopyInPreparationForSequoia.
                *req->mutable_serialized_tree() = std::move(subtree.SerializedValue);
                *req->mutable_schemas() = std::move(subtree.Schemas);
            }
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

    void MaterializeCopyPrerequisites()
    {
        if (SchemaIdToSchema_.empty()) {
            return;
        }

        YT_LOG_DEBUG("Started materializing copy prerequisites (SchemaCount: %v)",
            SchemaIdToSchema_.size());

        auto proxy = CreateObjectServiceWriteProxy(Client_, DstCellTag_);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TMasterYPathProxy::MaterializeCopyPrerequisites();
        GenerateMutationId(req);
        SetTransactionId(req, Transaction_->GetId());
        for (const auto& [schemaId, schema] : SchemaIdToSchema_) {
            auto* subreq = req->add_schema_id_to_schema_mapping();
            ToProto(subreq->mutable_schema_id(), schemaId);
            ToProto(subreq->mutable_schema(), schema);
        }
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Failed to materialize copy prerequisites (table schemas) on destination");

        auto rspOrError = batchRspOrError.Value()->GetResponse<TMasterYPathProxy::TRspMaterializeCopyPrerequisites>(0);
        auto rsp = rspOrError.Value();

        for (auto entry : rsp->updated_schema_id_mapping()) {
            auto oldSchemaId = FromProto<TMasterTableSchemaId>(entry.old_schema_id());
            auto newSchemaId = FromProto<TMasterTableSchemaId>(entry.new_schema_id());
            EmplaceOrCrash(SrcToDstSchemaIdMapping_, oldSchemaId, newSchemaId);
        }

        // Freeing up some space, materialized schemas are not needed anymore.
        SchemaIdToSchema_.clear();

        YT_LOG_DEBUG("Finished materializing prerequisites (SrcToDstSchemaIdMapping: %v)",
            SrcToDstSchemaIdMapping_);
    }

    template <class TOptions>
    void MaterializeNodes(const TOptions& options, std::optional<TNodeId> portalExitId = std::nullopt)
    {
        YT_LOG_DEBUG("Started materializing nodes (DstCellTag: %v)",
            DstCellTag_);

        auto proxy = CreateObjectServiceWriteProxy(Client_, DstCellTag_);
        auto batchReq = proxy.ExecuteBatch();
        for (const auto& [nodeId, data] : NodeIdToSerializedData_) {
            auto req = TMasterYPathProxy::MaterializeNode();
            SetTransactionId(req, Transaction_->GetId());
            GenerateMutationId(req);

            auto* serializedNode = req->mutable_serialized_node();
            serializedNode->CopyFrom(data->serialized_node());
            req->set_version(ProtocolVersion_);

            if (portalExitId && nodeId == SrcNodeId_) {
                ToProto(req->mutable_existing_node_id(), *portalExitId);
            }

            if (auto inheritedAttributesOverrideIt = NodeToInheritedAttributesOverride_.find(FromProto<TNodeId>(serializedNode->node_id()));
                inheritedAttributesOverrideIt != NodeToInheritedAttributesOverride_.end()) {
                ToProto(req->mutable_inherited_attributes_override(), *inheritedAttributesOverrideIt->second);
            }

            if (serializedNode->has_schema_id()) {
                auto oldSchemaId = FromProto<TMasterTableSchemaId>(serializedNode->schema_id());
                auto newSchemaId = GetOrCrash(SrcToDstSchemaIdMapping_, oldSchemaId);
                ToProto(serializedNode->mutable_schema_id(), newSchemaId);
            }

            if (!options.PreserveAccount) {
                ToProto(req->mutable_new_account_id(), NewAccountId_);
            }

            req->set_preserve_creation_time(options.PreserveCreationTime);
            req->set_preserve_expiration_time(options.PreserveExpirationTime);
            req->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
            req->set_preserve_owner(options.PreserveOwner);
            req->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
            req->set_enable_cross_cell_copying(options.EnableCrossCellCopying);

            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Failed to materialize nodes on destination");

        auto rspsOrError = batchRspOrError.Value()->GetResponses<TMasterYPathProxy::TRspMaterializeNode>();
        for (auto rspOrError : rspsOrError) {
            auto rsp = rspOrError.Value();
            auto oldNodeId = FromProto<TNodeId>(rsp->old_node_id());
            auto newNodeId = FromProto<TNodeId>(rsp->new_node_id());
            EmplaceOrCrash(SrcToDstNodeIdMapping_, oldNodeId, newNodeId);
        }

        YT_LOG_DEBUG("Nodes materialized (SrcToDstNodeIdMapping: %v)",
            SrcToDstNodeIdMapping_);
    }

    template <class TOptions>
    void AssembleTreeCopy(const TYPath& dstPath, const TOptions& options, bool inplace = false)
    {
        YT_LOG_DEBUG("Assembling tree copy (Inplace: %v)",
            inplace);

        auto proxy = CreateObjectServiceWriteProxy(Client_);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TCypressYPathProxy::AssembleTreeCopy(dstPath);
        SetTransactionId(req, Transaction_->GetId());
        GenerateMutationId(req);

        auto rootNodeId = GetOrCrash(SrcToDstNodeIdMapping_, SrcNodeId_);
        ToProto(req->mutable_root_node_id(), rootNodeId);

        req->set_inplace(inplace);

        for (const auto& nodeIdToChildren : NodeIdToChildrenMapping_) {
            auto updatedNodeId = GetOrCrash(SrcToDstNodeIdMapping_, nodeIdToChildren.NodeId);

            auto* entry = req->add_node_id_to_children();
            ToProto(entry->mutable_node_id(), updatedNodeId);

            for (auto child : nodeIdToChildren.Children) {
                auto updatedChildId = GetOrCrash(SrcToDstNodeIdMapping_, child.Id);
                auto* childEntry = entry->add_children();
                childEntry->set_key(child.Key);
                ToProto(childEntry->mutable_id(), updatedChildId);
            }
        }

        SetAssembleTreeCopyRequestParameters(req, options);
        batchReq->AddRequest(req);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError),
            "Failed to assemble tree copy on destination");

        const auto& batchRsp = batchRspOrError.Value();
        const auto& rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspAssembleTreeCopy>(0);
        const auto& rsp = rspOrError.Value();
        DstNodeId_ = FromProto<TNodeId>(rsp->node_id());

        YT_LOG_DEBUG("Finished assembling tree copy (DstNodeId: %v)",
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

        // NB: Failing to commit still means we shouldn't try to abort.
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

        auto useNewCopyPipeline = !BeginCopy(SrcPath_, Options_, true);
        if (useNewCopyPipeline) {
            YT_LOG_DEBUG("BeginCopy is deprecated, switching to the new copy pipeline");
            if (!LockCopyDestination(DstPath_, Options_)) {
                YT_VERIFY(DstNodeId_);
                return DstNodeId_;
            }

            LockCopySource(Options_, true);
            GetSerializedNodes(Options_);
            CalculateInheritedAttributes(SrcPath_);
        }

        SyncExternalCellsWithSourceNodeCell();

        if (useNewCopyPipeline) {
            MaterializeCopyPrerequisites();
            MaterializeNodes(Options_);
            AssembleTreeCopy(DstPath_, Options_);
        } else {
            EndCopy(DstPath_, Options_, false);
        }

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
        , Options_(options)
    {
        DstCellTag_ = cellTag;
    }

    void Run()
    {
        YT_LOG_DEBUG("Node externalization started");
        StartTransaction(
            Format("Externalize %v to %v", Path_, DstCellTag_),
            Options_);
        RequestAclAndAnnotation();

        auto useNewCopyPipeline = !BeginCopy(Path_, GetOptions(), false);
        if (useNewCopyPipeline) {
            YT_LOG_DEBUG("BeginCopy is deprecated, switching to the new copy pipeline");
            auto shouldContinue = LockCopyDestination(Path_, GetOptions());
            YT_VERIFY(shouldContinue);

            LockCopySource(GetOptions(), false);
            GetSerializedNodes(GetOptions());
            CalculateInheritedAttributes(Path_);
        }

        SyncExternalCellsWithSourceNodeCell();

        if (TypeFromId(SrcNodeId_) != EObjectType::MapNode) {
            THROW_ERROR_EXCEPTION("%v is not a map node", Path_);
        }

        auto portalExitId = CreatePortal();

        if (useNewCopyPipeline) {
            MaterializeCopyPrerequisites();
            MaterializeNodes(GetOptions(), portalExitId);
            SyncExitCellWithEntranceCell();
            AssembleTreeCopy(Path_, GetOptions(), /*inplace*/ true);
        } else {
            EndCopy(Path_, GetOptions(), /*inplace*/ true);
        }
        SyncExternalCellsWithClonedNodeCell();
        CommitTransaction();
        YT_LOG_DEBUG("Node externalization completed");
    }

private:
    const TYPath Path_;
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
        options.PreserveAcl = true;
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

    TNodeId CreatePortal()
    {
        YT_LOG_DEBUG("Creating portal");

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("exit_cell_tag", DstCellTag_);
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

        auto portalEntranceId = nodeIdOrError.Value();
        auto portalExitId = ReplaceCellTagInId(ReplaceTypeInId(portalEntranceId, EObjectType::PortalExit), DstCellTag_);

        YT_LOG_DEBUG("Portal created (EntranceId: %v, ExitId: %v)",
            portalEntranceId,
            portalExitId);

        return portalExitId;
    }

    void SyncExitCellWithEntranceCell()
    {
        YT_LOG_DEBUG("Synchronizing exit cell with entrance cell");

        const auto& connection = Client_->GetNativeConnection();
        auto future = connection->SyncHiveCellWithOthers(
            {connection->GetMasterCellId(CellTagFromId(SrcNodeId_))},
            connection->GetMasterCellId(DstCellTag_));

        auto error = WaitFor(future);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error synchronizing exit cell with entrance cell");

        YT_LOG_DEBUG("Exit cell synchronized with entrance cell");
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
    req->set_type(ToProto(type));
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
        protoSubrequest->set_attribute(ToProto(attribute));
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
    req->set_mode(ToProto(mode));
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
        FromProto<NHydra::TRevision>(rsp->revision()),
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
            THROW_ERROR_EXCEPTION("Target %v for the link %v does not exist",
                srcPath,
                dstPath);
        }
    }

    auto batchReq = proxy.ExecuteBatch();
    SetSuppressUpstreamSyncs(batchReq, options);
    SetPrerequisites(batchReq, options);

    auto req = TCypressYPathProxy::Create(dstPath);
    req->set_type(ToProto(EObjectType::Link));
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

struct TNodeConcatenatorTag
{ };

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
        TransactionId_ = Client_->GetTransactionId(options, /*allowNullTransaction*/ true);
        Append_ = dstPath.GetAppend();

        try {
            InitializeObjects(srcPaths, dstPath);
            StartNestedInputTransactions();
            GetObjectAttributes();
            InferCommonType();
            // COMPAT(ignat): drop in 25.1; chunk counts must be available in GetBasicAttributes response.
            GetSrcObjectChunkCounts();
            if (CommonType_ == EObjectType::Table) {
                InferOutputTableSchema();
            }
            FetchChunkSpecs();
            if (Sorted_) {
                ValidateChunkSchemas();
                SortChunks();
                ValidateChunkRanges();
                if (Append_) {
                    FetchAndValidateBoundaryKeys();
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

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TNodeConcatenatorTag());

    void InitializeObjects(
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
            req->set_permission(ToProto(EPermission::FullRead));
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
                // COMPAT(ignat): write the following in 25.1
                // srcObject->Type = FromProto<EObjectType>(rsp->type());
                srcObject->ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
                srcObject->ExternalTransactionId = rsp->has_external_transaction_id()
                    ? FromProto<TTransactionId>(rsp->external_transaction_id())
                    : *srcObject->TransactionId;
                srcObject->SecurityTags = FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
                if (rsp->has_chunk_count()) {
                    srcObject->ChunkCount = rsp->chunk_count();
                }

                // COMPAT(ignat): log object type in 25.1
                YT_LOG_DEBUG(
                    "Source object attributes received "
                    "(Path: %v, ObjectId: %v, ExternalCellTag: %v, SecurityTags: %v, ExternalTransactionId: %v, ChunkCount: %v)",
                    srcObject->GetPath(),
                    srcObject->ObjectId,
                    srcObject->ExternalCellTag,
                    srcObject->SecurityTags,
                    srcObject->ExternalTransactionId,
                    srcObject->ChunkCount);
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

    void InferCommonType()
    {
        auto isValidType = [&] (EObjectType type) {
            return type == EObjectType::Table || type == EObjectType::File;
        };

        DstObject_.Type = TypeFromId(DstObject_.ObjectId);

        // COMPAT(ignat): drop this logic in 25.1 since type is fully supported in GetBasicAttributes.
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
        bool hasMissingChunkCounts = false;
        for (auto& srcObject : SrcObjects_) {
            if (srcObject.ChunkCount == TUserObject::UndefinedChunkCount) {
                hasMissingChunkCounts = true;
                break;
            }
        }

        if (!hasMissingChunkCounts) {
            YT_LOG_DEBUG("Skip fetching chunk counts of source objects");
            return;
        }

        YT_LOG_DEBUG("Fetch chunk counts of source objects");

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

        YT_LOG_DEBUG("Source objects chunk counts received");
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
                {.IgnoreSortOrder = false});
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

    void SortChunks()
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

    void FetchAndValidateBoundaryKeys()
    {
        YT_LOG_DEBUG("Fetch and validate boundary keys of destination table");

        auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());

        auto request = TTableYPathProxy::Get(DstObject_.GetObjectIdPath() + "/@boundary_keys");
        AddCellTagToSyncWith(request, DstObject_.ObjectId);
        NCypressClient::SetTransactionId(request, *DstObject_.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(request));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to fetch boundary keys of destination table %v",
            DstObject_.GetPath());

        YT_LOG_DEBUG("Boundary keys of destination table received");

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
        req->set_update_mode(ToProto(Append_ ? EUpdateMode::Append : EUpdateMode::Overwrite));
        req->set_lock_mode(ToProto(Append_ ? ELockMode::Shared : ELockMode::Exclusive));
        if (CommonType_ == EObjectType::Table) {
            ToProto(req->mutable_table_schema(), OutputTableSchema_);
            req->set_schema_mode(ToProto(OutputTableSchemaMode_));
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
        req->set_upload_transaction_timeout(ToProto(Client_->Connection_->GetConfig()->UploadTransactionTimeout));
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

        // COMPAT(h0pless): remove this when all masters are 24.2.
        if (CommonType_ == EObjectType::Table) {
            req->set_schema_mode(ToProto(OutputTableSchemaMode_));
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

            return TKey::FromRowUnchecked(RowBuffer_->CaptureRow(TRange(row.Begin(), keyColumnCount)), keyColumnCount);
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

    TNodeConcatenator(this, Logger)
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

// COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
void TClient::DoInternalizeNode(
    const TYPath& /*path*/,
    const TInternalizeNodeOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("Node internalization is deprecated and is no longer possible.");
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
