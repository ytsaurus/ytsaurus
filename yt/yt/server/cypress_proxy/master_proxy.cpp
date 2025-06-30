#include "master_proxy.h"

#include "bootstrap.h"
#include "helpers.h"
#include "node_proxy_base.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/misc/range_formatters.h>


namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
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
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeCopyPrerequisites);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeNode);

    bool DoInvoke(const ISequoiaServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeCopyPrerequisites);
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeNode);

        InvokeResult_ = EInvokeResult::ForwardToMaster;
        // Note that DoInvoke of the base class is not called here!
        // This is done to avoid throwing an extra error during Invoke().
        return true;
    }

    TObjectServiceProxy CreateWriteProxy(TCellTag cellTag)
    {
        return TObjectServiceProxy::FromDirectMasterChannel(
            GetNativeAuthenticatedClient()->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag));
    }
};

DEFINE_YPATH_SERVICE_METHOD(TMasterProxy, MaterializeCopyPrerequisites)
{
    if (!request->sequoia_destination()) {
        InvokeResult_ = EInvokeResult::ForwardToMaster;
        return;
    }

    SequoiaSession_->ValidateTransactionPresence();

    auto transactionId = SequoiaSession_->GetCurrentCypressTransactionId();

    context->SetRequestInfo("TransactionId: %v, SchemaCount: %v, OldSchemaIds: %v",
        transactionId,
        request->schema_descriptors_size(),
        std::views::transform(
            request->schema_descriptors(),
            [] (const auto& entry) {
                return FromProto<TMasterTableSchemaId>(entry.schema_id());
            }));

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
            cellTagToSchemaIds[cellTag].emplace(std::move(schemaId));
        }

        auto [_, emplaced] = schemaDescriptors.emplace(schemaId, entry.mutable_schema());
        YT_LOG_ALERT_AND_THROW_UNLESS(emplaced,
            "Duplicate schema ID received during copy prerequisite materialization (SchemaId: %v)",
            schemaId);
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
            subreq->mutable_schema()->Swap(schema);
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

    context->SetResponseInfo("SchemaIdMapping: %v, NodeIdToDstCellTagMapping: %v",
        MakeShrunkFormattableView(
            response->old_to_new_schema_id(),
            [] (
                TStringBuilderBase* builder,
                const auto& entry
            ) {
                auto oldId = FromProto<TMasterTableSchemaId>(entry.old_schema_id());
                auto newId = FromProto<TMasterTableSchemaId>(entry.new_schema_id());
                builder->AppendFormat("%v -> %v", oldId, newId);
            },
            /*limit*/ 100),
        MakeShrunkFormattableView(
            response->node_id_to_cell_tag(),
            [] (
                TStringBuilderBase* builder,
                const auto& entry
            ) {
                auto nodeId = FromProto<TNodeId>(entry.node_id());
                auto cellTag = FromProto<TCellTag>(entry.cell_tag());
                builder->AppendFormat("%v -> %v", nodeId, cellTag);
            },
            /*limit*/ 100));

    FinishSequoiaSessionAndReply(context, NullCellId, /*commitSession*/ false);
}

DEFINE_YPATH_SERVICE_METHOD(TMasterProxy, MaterializeNode)
{
    if (!request->sequoia_destination()) {
        InvokeResult_ = EInvokeResult::ForwardToMaster;
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

    context->SetRequestInfo("OldNodeId: %v, Mode: %v, Version: %v, InheritedAttributesOverride: %v, "
        "PreserveAccount: %v, PreserveCreationTime: %v, PreserveExpirationTime: %v, "
        "PreserveExpirationTimeout: %v, PreserveOwner: %v, PessimisticQuotaCheck: %v, TransactionId: %v",
        oldNodeId,
        FromProto<ENodeCloneMode>(request->mode()),
        request->version(),
        inheritedAttributesOverride->ListPairs(),
        !request->has_new_account_id(),
        request->preserve_creation_time(),
        request->preserve_expiration_time(),
        request->preserve_expiration_timeout(),
        request->preserve_owner(),
        request->pessimistic_quota_check(),
        SequoiaSession_->GetCurrentCypressTransactionId());

    auto originalType = TypeFromId(oldNodeId);
    auto type = MaybeConvertToSequoiaType(originalType);
    if (originalType == EObjectType::PortalExit) {
        type = EObjectType::SequoiaMapNode;
    }

    // TODO(h0pless): Write type validation here when supporting the externalization.
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

    context->SetResponseInfo("OldNodeId: %v, NewNodeId: %v",
        oldNodeId,
        newNodeId);

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
