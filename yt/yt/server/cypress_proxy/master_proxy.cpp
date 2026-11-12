#include "master_proxy.h"

#include "bootstrap.h"
#include "helpers.h"
#include "node_proxy_base.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/server/lib/table_server/proto/table_manager.pb.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/yt/misc/range_formatters.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTableClient;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterProxy
    : public TNodeProxyBase
{
public:
    using TNodeProxyBase::TNodeProxyBase;

protected:
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObject);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeCopyPrerequisites);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeNode);

    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeCopyPrerequisites);
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeNode);

        InvokeResult_ = TForwardToMasterPayload{};
        // Note that DoInvoke of the base class is not called here!
        // This is done to avoid throwing an extra error during Invoke().
        return true;
    }

    TObjectServiceProxy CreateWriteProxy(TCellTag cellTag)
    {
        return TObjectServiceProxy::FromDirectMasterChannel(
            SequoiaSession_
                ->GetNativeAuthenticatedClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag));
    }
};

DEFINE_YPATH_SERVICE_METHOD(TMasterProxy, CreateObject)
{
    auto type = FromProto<EObjectType>(request->type());
    if (type != EObjectType::SecondaryIndex) {
        InvokeResult_ = TForwardToMasterPayload{};
        return;
    }

    context->AnnotateRequest()
        .With("Type", type);

    THROW_ERROR_EXCEPTION_IF(SequoiaSession_->GetCurrentCypressTransactionId(),
        "Cannot create secondary index within a transaction");

    auto attributes = request->has_object_attributes()
        ? NYTree::FromProto(request->object_attributes())
        : nullptr;

    THROW_ERROR_EXCEPTION_UNLESS(attributes, "Attributes missing during secondary index creation");

    auto tableId = attributes->Get<TTableId>("table_id");
    auto indexTableId = attributes->Get<TTableId>("index_table_id");
    auto tableCellTag = CellTagFromId(tableId);
    auto indexTableCellTag = CellTagFromId(indexTableId);

    SequoiaSession_->AcquireCypressLockInSequoia(
        tableId,
        ELockMode::Exclusive);

    SequoiaSession_->AcquireCypressLockInSequoia(
        indexTableId,
        ELockMode::Exclusive);

    const auto& transaction = SequoiaSession_->SequoiaTransaction();

    auto hintId = transaction->GenerateObjectId(type, tableCellTag, /*sequoia*/ true);

    NTableServer::NProto::TReqCreateSecondaryIndex req;
    ToProto(req.mutable_object_attributes(), *attributes);
    ToProto(req.mutable_hint_id(), hintId);

    // Secondary index is instantiated on indexed table native cell (1) and
    // index table native cell (2), creation is replicated to
    // the indexed table external cell (3) and from that point
    // changes to the object and its removal are replicated from (1) to (2) and (3).
    transaction->AddTransactionAction(
        tableCellTag,
        NTransactionClient::MakeTransactionActionData(req));
    if (tableCellTag != indexTableCellTag) {
        transaction->AddTransactionAction(
            indexTableCellTag,
            NTransactionClient::MakeTransactionActionData(req));
    }

    // We totally rely on master accepting the hint here.
    ToProto(response->mutable_object_id(), hintId);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(tableId), /*commitSession*/ true);
}

DEFINE_YPATH_SERVICE_METHOD(TMasterProxy, MaterializeCopyPrerequisites)
{
    if (!request->sequoia_destination()) {
        InvokeResult_ = TForwardToMasterPayload{};
        return;
    }

    SequoiaSession_->ValidateTransactionPresence();

    auto transactionId = SequoiaSession_->GetCurrentCypressTransactionId();

    context->AnnotateRequest()
        .With("TransactionId", transactionId)
        .With("SchemaCount", request->schema_descriptors_size())
        .With("OldSchemaIds", std::views::transform( request->schema_descriptors(), [] (const auto& entry) { return FromProto<TMasterTableSchemaId>(entry.schema_id()); }));

    auto addNodeIdMapping = [&response] (TNodeId nodeId, TCellTag cellTag) {
        auto* nodeIdMapping = response->add_node_id_to_cell_tag();
        ToProto(nodeIdMapping->mutable_node_id(), nodeId);
        nodeIdMapping->set_cell_tag(ToProto(cellTag));
    };

    THashMap<TCellTag, THashSet<TMasterTableSchemaId>> cellTagToSchemaIds;
    THashMap<TMasterTableSchemaId, NTableClient::NProto::TTableSchemaExt*> schemaDescriptors;
    schemaDescriptors.reserve(request->schema_descriptors_size());
    for (auto& entry : *request->mutable_schema_descriptors()) {
        auto nodeIds = FromProto<std::vector<TNodeId>>(entry.node_ids());
        auto schemaId = FromProto<TMasterTableSchemaId>(entry.schema_id());

        for (auto nodeId : nodeIds) {
            auto cellTag = SequoiaSession_->SequoiaTransaction()->GetRandomSequoiaNodeHostCellTag();
            addNodeIdMapping(nodeId, cellTag);
            cellTagToSchemaIds[cellTag].emplace(schemaId);
        }

        auto [_, emplaced] = schemaDescriptors.emplace(schemaId, entry.mutable_schema());
        YT_TLOG_ALERT_AND_THROW_UNLESS(
            emplaced,
            "Duplicate schema ID received during copy prerequisite materialization")
            .With("SchemaId", schemaId);
    }

    for (auto nodeId : FromProto<std::vector<TNodeId>>(request->schemaless_node_ids())) {
        auto cellTag = SequoiaSession_->SequoiaTransaction()->GetRandomSequoiaNodeHostCellTag();
        addNodeIdMapping(nodeId, cellTag);
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> futures;
    futures.reserve(cellTagToSchemaIds.size());
    for (const auto& [cellTag, schemaIds] : cellTagToSchemaIds) {
        auto proxy = CreateWriteProxy(cellTag);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TMasterYPathProxy::MaterializeCopyPrerequisites();
        GenerateMutationId(req);

        SetTransactionId(req, transactionId);
        req->set_sequoia_destination(false);

        for (const auto& schemaId : schemaIds) {
            auto* schema = GetOrCrash(schemaDescriptors, schemaId);
            auto* subreq = req->add_schema_descriptors();
            ToProto(subreq->mutable_schema_id(), schemaId);
            subreq->mutable_schema()->CopyFrom(*schema);
        }
        batchReq->AddRequest(req);

        futures.push_back(batchReq->Invoke());
    }

    auto batchRspsOrError = WaitFor(AllSucceeded(futures));
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspsOrError,
        "Failed to materialize copy prerequisites on master");

    response->mutable_old_to_new_schema_id()->Reserve(request->schema_descriptors_size());
    for (const auto& batchRsp : batchRspsOrError.Value()) {
        auto rspOrError = batchRsp->GetResponse<TMasterYPathProxy::TRspMaterializeCopyPrerequisites>(0);
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError,
            "Failed to materialize copy prerequisites on master");

        auto rsp = rspOrError.Value();

        for (auto entry : rsp->old_to_new_schema_id()) {
            auto oldSchemaId = FromProto<TMasterTableSchemaId>(entry.old_schema_id());
            auto newSchemaId = FromProto<TMasterTableSchemaId>(entry.new_schema_id());

            auto* rspEntry = response->add_old_to_new_schema_id();
            ToProto(rspEntry->mutable_old_schema_id(), oldSchemaId);
            ToProto(rspEntry->mutable_new_schema_id(), newSchemaId);
        }
    }

    context->AnnotateResponse()
        .With("SchemaIdMapping", MakeShrunkFormattableView( response->old_to_new_schema_id(), [] ( TStringBuilderBase* builder, const auto& entry ) { auto oldId = FromProto<TMasterTableSchemaId>(entry.old_schema_id()); auto newId = FromProto<TMasterTableSchemaId>(entry.new_schema_id()); builder->AppendFormat("%v -> %v", oldId, newId); }, /*limit*/ 100))
        .With("NodeIdToDstCellTagMapping", MakeShrunkFormattableView( response->node_id_to_cell_tag(), [] ( TStringBuilderBase* builder, const auto& entry ) { auto nodeId = FromProto<TNodeId>(entry.node_id()); auto cellTag = FromProto<TCellTag>(entry.cell_tag()); builder->AppendFormat("%v -> %v", nodeId, cellTag); }, /*limit*/ 100));

    FinishSequoiaSessionAndReply(context, NullCellId, /*commitSession*/ false);
}

DEFINE_YPATH_SERVICE_METHOD(TMasterProxy, MaterializeNode)
{
    if (!request->sequoia_destination()) {
        InvokeResult_ = TForwardToMasterPayload{};
        return;
    }

    SequoiaSession_->ValidateTransactionPresence();

    auto oldNodeId = FromProto<TNodeId>(request->serialized_node().node_id());
    auto existingNodeId = FromProto<TNodeId>(request->existing_node_id());

    // For now this field is only used for logging and passed to master directly from
    // request. Once inheritable attributes are implemented, a revision of this code will be
    // in order.
    // TODO(h0pless): Do that.
    auto inheritedAttributesOverride = request->has_inherited_attributes_override()
        ? NYTree::FromProto(request->inherited_attributes_override())
        : CreateEphemeralAttributes();

    auto accountId = request->new_account_id();
    auto cellTagHint = request->has_cell_tag_hint()
        ? FromProto<TCellTag>(request->cell_tag_hint())
        : InvalidCellTag;

    context->AnnotateRequest()
        .With("OldNodeId", oldNodeId)
        .With("Mode", FromProto<ENodeCloneMode>(request->mode()))
        .With("Version", request->version())
        .With("InheritedAttributesOverride", inheritedAttributesOverride->ListPairs())
        .With("PreserveAccount", !request->has_new_account_id())
        .With("PreserveCreationTime", request->preserve_creation_time())
        .With("PreserveExpirationTime", request->preserve_expiration_time())
        .With("PreserveExpirationTimeout", request->preserve_expiration_timeout())
        .With("PreserveOwner", request->preserve_owner())
        .With("PessimisticQuotaCheck", request->pessimistic_quota_check())
        .With("TransactionId", SequoiaSession_->GetCurrentCypressTransactionId());

    auto originalType = TypeFromId(oldNodeId);
    auto type = MaybeConvertToSequoiaType(originalType);
    if (originalType == EObjectType::PortalExit) {
        type = EObjectType::SequoiaMapNode;
    }

    // TODO(h0pless): Write type validation here when supporting the externalization.
    // For more details see YT-26855.
    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION("Creation of %Qlv is not supported in Sequoia yet",
            type);
    }

    auto newNodeId = SequoiaSession_->MaterializeNodeOnMaster(
        request,
        cellTagHint,
        type,
        existingNodeId);

    ToProto(response->mutable_old_node_id(), oldNodeId);
    ToProto(response->mutable_new_node_id(), newNodeId);

    context->AnnotateResponse()
        .With("NewNodeId", newNodeId);

    FinishSequoiaSessionAndReply(context, CellIdFromObjectId(newNodeId), /*commitSession*/ true);
}

////////////////////////////////////////////////////////////////////////////////

INodeProxyPtr CreateMasterProxy(
    IBootstrap* bootstrap,
    TSequoiaSessionPtr session)
{
    return New<TMasterProxy>(bootstrap, std::move(session));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
