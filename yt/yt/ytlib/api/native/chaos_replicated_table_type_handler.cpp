#include "chaos_replicated_table_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"
#include "connection.h"
#include "transaction.h"

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NChaosClient;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TChaosReplicatedTableTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TObjectId> CreateNode(
        EObjectType type,
        const TYPath& tablePath,
        const TCreateNodeOptions& options) override
    {
        if (type != EObjectType::ChaosReplicatedTable) {
            return {};
        }

        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
        if (attributes->Contains("replication_card_id")) {
            return Client_->CreateNodeImpl(type, tablePath, *attributes, options);
        }

        if (!attributes->Get<bool>("owns_replication_card", true)) {
            THROW_ERROR_EXCEPTION("\"owns_replication_card\" cannot be false while \"replication_card_id\" is missing");
        }

        auto replicatedTableOptions = attributes->FindAndRemove<TReplicatedTableOptionsPtr>("replicated_table_options");

        auto clusterName = GetClusterName();

        auto transaction = StartTransaction(tablePath);

        auto adjustedOptions = AdjustCreateNodeOptions(transaction, options);

        auto tableId = Client_->CreateNodeImpl(
            type,
            tablePath,
            *attributes,
            adjustedOptions);

        auto chaosMetadataCellId = GetChaosMetadataCellId(transaction, tableId);

        auto replicationCardId = GenerateReplicationCardId(chaosMetadataCellId);

        AddTransactionActions(
            transaction,
            tableId,
            tablePath,
            clusterName,
            replicatedTableOptions,
            replicationCardId,
            chaosMetadataCellId);

        CommitTransaction(transaction);

        return tableId;
    }

private:
    TClient* const Client_;


    ITransactionPtr StartTransaction(const TYPath& path)
    {
        TNativeTransactionStartOptions options;
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", Format("Creating %v", path));
        options.Attributes = std::move(transactionAttributes);
        options.SuppressStartTimestampGeneration = true;
        options.StartCypressTransaction = false;
        return WaitFor(Client_->StartNativeTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    TCreateNodeOptions AdjustCreateNodeOptions(const ITransactionPtr& transaction, const TCreateNodeOptions& options)
    {
        auto adjustedOptions = options;
        adjustedOptions.TransactionId = transaction->GetId();
        return adjustedOptions;
    }

    TReplicationCardId GenerateReplicationCardId(TCellId chaosMetadataCellId)
    {
        auto channel = Client_->GetChaosChannelByCellId(chaosMetadataCellId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.GenerateReplicationCardId();

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TReplicationCardId>(rsp->replication_card_id());
    }

    TString GetClusterName()
    {
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::LocalCache;
        auto yson = WaitFor(Client_->GetNode("//sys/@cluster_name", options))
            .ValueOrThrow();
        return ConvertTo<TString>(yson);
    }

    template <class T>
    T GetAttributeOrThrow(
        const ITransactionPtr& transaction,
        const TYPath& path,
        const TString& attributeKey,
        const TString& errorMessage)
    {
        TGetNodeOptions options;
        options.Attributes = {attributeKey};
        auto yson = WaitFor(transaction->GetNode(path, options))
            .ValueOrThrow();

        auto node = ConvertToNode(yson);
        const auto& attributes = node->Attributes();
        auto optionalAttributeValue = attributes.Find<T>(attributeKey);
        if (!optionalAttributeValue) {
            THROW_ERROR_EXCEPTION(errorMessage);
        }

        return *optionalAttributeValue;
    }

    TString GetChaosCellBundle(const ITransactionPtr& transaction, TTableId tableId)
    {
        auto yson = WaitFor(transaction->GetNode(Format("#%v/@chaos_cell_bundle", tableId)))
            .ValueOrThrow();
        return ConvertTo<TString>(yson);
    }

    TCellId GetChaosMetadataCellId(const ITransactionPtr& transaction, TTableId tableId)
    {
        auto chaosCellBundle = GetChaosCellBundle(transaction, tableId);

        return GetAttributeOrThrow<TCellId>(
            transaction,
            Format("//sys/chaos_cell_bundles/%v", ToYPathLiteral(chaosCellBundle)),
            "metadata_cell_id",
            Format("Chaos cell bundle %Qv has no associated metadata chaos cell", chaosCellBundle));
    }

    void AddTransactionActions(
        const ITransactionPtr& transaction,
        TTableId tableId,
        const TYPath& tablePath,
        const TString& tableClusterName,
        TReplicatedTableOptionsPtr replicatedTableOptions,
        TReplicationCardId replicationCardId,
        TCellId metadataChaosCellId)
    {
        NChaosClient::NProto::TReqCreateReplicationCard request;
        ToProto(request.mutable_hint_id(), replicationCardId);
        ToProto(request.mutable_table_id(), tableId);
        request.set_table_path(tablePath);
        request.set_table_cluster_name(tableClusterName);
        if (replicatedTableOptions) {
            request.set_replicated_table_options(ConvertToYsonString(replicatedTableOptions).ToString());
        }

        const auto& connection = Client_->GetNativeConnection();
        auto nodeCellTag = CellTagFromId(tableId);
        auto masterCellId = connection->GetMasterCellId(nodeCellTag);

        auto actionData = MakeTransactionActionData(request);
        transaction->AddAction(masterCellId, actionData);
        transaction->AddAction(metadataChaosCellId, actionData);
    }

    void CommitTransaction(const ITransactionPtr& transaction)
    {
        TTransactionCommitOptions options;
        options.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
        options.GeneratePrepareTimestamp = false;
        WaitFor(transaction->Commit(options))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateChaosReplicatedTableTypeHandler(TClient* client)
{
    return New<TChaosReplicatedTableTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
