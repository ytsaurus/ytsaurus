#include "chaos_replicated_table_node_proxy.h"

#include "chaos_cell_bundle.h"
#include "chaos_manager.h"
#include "chaos_replicated_table_node.h"

#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChaosServer {

using namespace NApi;
using namespace NCellMaster;
using namespace NChaosClient;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TChaosReplicatedTableNode>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TChaosReplicatedTableNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* impl = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChaosCellBundle)
            .SetWritable(true)
            .SetReplicated(true)
            .SetPresent(IsObjectAlive(impl->ChaosCellBundle())));
        descriptors->push_back(EInternedAttributeKey::Dynamic);
        descriptors->push_back(EInternedAttributeKey::ReplicationCardId);
        descriptors->push_back(EInternedAttributeKey::OwnsReplicationCard);
        descriptors->push_back(EInternedAttributeKey::Era);
        descriptors->push_back(EInternedAttributeKey::CoordinatorCellIds);
        descriptors->push_back(EInternedAttributeKey::Replicas);
        descriptors->push_back(EInternedAttributeKey::ReplicationCollocationId);
        descriptors->push_back(EInternedAttributeKey::ReplicatedTableOptions);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Schema)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TreatAsQueueConsumer)
            .SetWritable(true)
            .SetPresent(impl->HasNonEmptySchema() && impl->IsSorted()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        const auto* trunkNode = node->GetTrunkNode();

        switch (key) {
            case EInternedAttributeKey::ChaosCellBundle:
                if (const auto& bundle = trunkNode->ChaosCellBundle()) {
                    BuildYsonFluently(consumer)
                        .Value(bundle->GetName());
                    return true;
                } else {
                    return false;
                }

            case EInternedAttributeKey::Dynamic:
                BuildYsonFluently(consumer)
                    .Value(true);
                return true;

            case EInternedAttributeKey::ReplicationCardId:
                BuildYsonFluently(consumer)
                    .Value(node->GetReplicationCardId());
                return true;

            case EInternedAttributeKey::OwnsReplicationCard:
                BuildYsonFluently(consumer)
                    .Value(node->GetOwnsReplicationCard());
                return true;

            case EInternedAttributeKey::TreatAsQueueConsumer: {
                if (!node->HasNonEmptySchema() || !node->IsSorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetTreatAsConsumer());
                return true;
            }

            default:
                break;
        }

        return TCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        switch (key) {
            case EInternedAttributeKey::ChaosCellBundle: {
                ValidateNoTransaction();

                auto name = ConvertTo<TString>(value);

                const auto& chaosManager = Bootstrap_->GetChaosManager();
                auto* cellBundle = chaosManager->GetChaosCellBundleByNameOrThrow(name, true /*activeLifeStageOnly*/);

                auto* lockedImpl = LockThisImpl();
                chaosManager->SetChaosCellBundle(lockedImpl, cellBundle);

                return true;
            }

            case EInternedAttributeKey::OwnsReplicationCard: {
                ValidateNoTransaction();
                auto* lockedImpl = LockThisImpl();
                lockedImpl->SetOwnsReplicationCard(ConvertTo<bool>(value));
                return true;
            }

            case EInternedAttributeKey::TreatAsQueueConsumer: {
                ValidateNoTransaction();
                auto* lockedTableNode = LockThisImpl();
                if (!lockedTableNode->HasNonEmptySchema() || !lockedTableNode->IsSorted()) {
                    break;
                }
                bool isConsumerObjectBefore = lockedTableNode->IsTrackedConsumerObject();
                lockedTableNode->SetTreatAsConsumer(ConvertTo<bool>(value));
                bool isConsumerObjectAfter = lockedTableNode->IsTrackedConsumerObject();
                const auto& chaosManager = Bootstrap_->GetChaosManager();
                if (isConsumerObjectAfter && !isConsumerObjectBefore) {
                    chaosManager->RegisterConsumer(lockedTableNode);
                } else if (!isConsumerObjectAfter && isConsumerObjectBefore) {
                    chaosManager->UnregisterConsumer(lockedTableNode);
                }
                return true;
            }

            default:
                break;
        }

        return TCypressNodeProxyBase::SetBuiltinAttribute(key, value);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* table = GetThisImpl();
        const auto& timestampProvider = Bootstrap_->GetTimestampProvider();

        switch (key) {
            case EInternedAttributeKey::Era:
                return GetReplicationCard()
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->Era);
                    }));

            case EInternedAttributeKey::CoordinatorCellIds:
                return GetReplicationCard({.IncludeCoordinators = true})
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->CoordinatorCellIds);
                    }));

            case EInternedAttributeKey::Replicas: {
                auto options = TReplicationCardFetchOptions{
                    .IncludeProgress = true,
                    .IncludeReplicatedTableOptions = true,
                };
                auto latestTimestamp = timestampProvider->GetLatestTimestamp();

                return GetReplicationCard(options)
                    .Apply(BIND([=] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .DoMapFor(card->Replicas, [&] (TFluentMap fluent, const auto& pair) {
                                const auto& [replicaId, replica] = pair;
                                auto minTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
                                auto replicaLagTime = minTimestamp < latestTimestamp
                                    ? NTransactionClient::TimestampDiffToDuration(minTimestamp, latestTimestamp).second
                                    : TDuration::Zero();

                                fluent
                                    .Item(ToString(replicaId))
                                    .BeginMap()
                                        .Item("cluster_name").Value(replica.ClusterName)
                                        .Item("replica_path").Value(replica.ReplicaPath)
                                        .Item("state").Value(replica.State)
                                        .Item("mode").Value(replica.Mode)
                                        .Item("content_type").Value(replica.ContentType)
                                        .Item("replication_lag_timestamp").Value(minTimestamp)
                                        .Item("replication_lag_time").Value(replicaLagTime)
                                        .Item("replicated_table_tracker_enabled").Value(replica.EnableReplicatedTableTracker)
                                    .EndMap();

                            });
                    }));
            }

            case EInternedAttributeKey::Schema:
                if (!table->GetSchema()) {
                    break;
                }
                return table->GetSchema()->AsYsonAsync();

            case EInternedAttributeKey::ReplicatedTableOptions:
                return GetReplicationCard({.IncludeReplicatedTableOptions = true})
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->ReplicatedTableOptions);
                    }));

            case EInternedAttributeKey::ReplicationCollocationId:
                return GetReplicationCard()
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->ReplicationCardCollocationId);
                    }));

            default:
                break;
        }

        return TCypressNodeProxyBase::GetBuiltinAttributeAsync(key);
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
        DISPATCH_YPATH_SERVICE_METHOD(Alter);
        return TBase::DoInvoke(context);
    }

    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardFetchOptions& options = {})
    {
        const auto& connection = Bootstrap_->GetClusterConnection();
        auto clientOptions = TClientOptions::FromAuthenticationIdentity(NRpc::GetCurrentAuthenticationIdentity());
        auto client = connection->CreateClient(clientOptions);
        const auto* impl = GetThisImpl();
        TGetReplicationCardOptions getCardOptions;
        static_cast<TReplicationCardFetchOptions&>(getCardOptions) = options;
        getCardOptions.BypassCache = true;
        return client->GetReplicationCard(impl->GetReplicationCardId(), getCardOptions)
            .Apply(BIND([client] (const TReplicationCardPtr& card) {
                return card;
            }));
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter);
};

DEFINE_YPATH_SERVICE_METHOD(TChaosReplicatedTableNodeProxy, GetMountInfo)
{
    DeclareNonMutating();
    SuppressAccessTracking();

    context->SetRequestInfo();

    ValidateNotExternal();
    ValidateNoTransaction();

    const auto* trunkTable = GetThisImpl();

    auto* schema = trunkTable->GetSchema();
    if (!schema || schema->AsTableSchema()->Columns().empty()) {
        THROW_ERROR_EXCEPTION("Table schema is not specified");
    }
    if (!trunkTable->GetReplicationCardId()) {
        THROW_ERROR_EXCEPTION("Replication card id is not specified");
    }

    ToProto(response->mutable_table_id(), trunkTable->GetId());
    ToProto(response->mutable_upstream_replica_id(), NTabletClient::TTableReplicaId());
    ToProto(response->mutable_replication_card_id(), trunkTable->GetReplicationCardId());
    response->set_dynamic(true);
    ToProto(response->mutable_schema(), *trunkTable->GetSchema()->AsTableSchema());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TChaosReplicatedTableNodeProxy, Alter)
{
    DeclareMutating();

    NTableClient::TTableSchemaPtr schema;
    TMasterTableSchemaId schemaId;

    if (request->has_schema()) {
        schema = New<TTableSchema>(FromProto<TTableSchema>(request->schema()));
    }
    if (request->has_schema_id()) {
        schemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
    }
    if (request->has_dynamic() ||
        request->has_upstream_replica_id() ||
        request->has_schema_modification() ||
        request->has_replication_progress())
    {
        THROW_ERROR_EXCEPTION("Chaos replicated table could not be altered in this way");
    }

    context->SetRequestInfo("Schema: %v",
        schema);

    auto* table = LockThisImpl();

    const auto& tableManager = Bootstrap_->GetTableManager();
    // NB: Chaos replicated table is always native.
    auto schemaReceived = schemaId || schema;
    if (schemaReceived) {
        tableManager->ValidateTableSchemaCorrespondence(
            table->GetVersionedId(),
            schema,
            schemaId);
    }

    TTableSchemaPtr effectiveSchema;
    if (schema) {
        effectiveSchema = schema;
    } else if (schemaId) {
        effectiveSchema = tableManager->GetMasterTableSchemaOrThrow(schemaId)->AsTableSchema();
    } else {
        effectiveSchema = table->GetSchema()->AsTableSchema();
    }

    // NB: Sorted dynamic tables contain unique keys, set this for user.
    if (schemaReceived && effectiveSchema->IsSorted() && !effectiveSchema->GetUniqueKeys()) {
        effectiveSchema = effectiveSchema->ToUniqueKeys();
    }

    if (schemaReceived) {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();

        if (!config->EnableDescendingSortOrder || !config->EnableDescendingSortOrderDynamic) {
            ValidateNoDescendingSortOrder(*effectiveSchema);
        }
    }

    if (table->IsTrackedConsumerObject()) {
        bool isValidConsumerSchema = !effectiveSchema->IsEmpty() && effectiveSchema->IsSorted();
        if (!isValidConsumerSchema) {
            THROW_ERROR_EXCEPTION(
                "Chaos replicated table object cannot be both a queue and a consumer.\
                To transform consumer into queue set `treat_as_queue_consumer` attribute into False first");
        }
    }

    YT_LOG_ACCESS(
        context,
        GetId(),
        GetPath(),
        Transaction_);

    bool isQueueObjectBefore = table->IsTrackedQueueObject();

    tableManager->GetOrCreateNativeMasterTableSchema(*effectiveSchema, table);

    bool isQueueObjectAfter = table->IsTrackedQueueObject();
    const auto& chaosManager = Bootstrap_->GetChaosManager();
    if (!isQueueObjectBefore && isQueueObjectAfter) {
        chaosManager->RegisterQueue(table);
    } else if (isQueueObjectBefore && !isQueueObjectAfter) {
        chaosManager->UnregisterQueue(table);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateChaosReplicatedTableNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TChaosReplicatedTableNode* trunkNode)
{
    return New<TChaosReplicatedTableNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
