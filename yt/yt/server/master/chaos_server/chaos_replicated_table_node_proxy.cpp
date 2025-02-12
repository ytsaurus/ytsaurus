#include "chaos_replicated_table_node_proxy.h"

#include "chaos_cell_bundle.h"
#include "chaos_manager.h"
#include "chaos_replicated_table_node.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/table_server/helpers.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

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
using namespace NTabletClient;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

// TODO(osidorkin): Remove after YT-17817
class TChaosReplicatedTableTabletsCountGetter
{
public:
    static TFuture<TYsonString> GetTabletsCountYson(
        TFuture<TReplicationCardPtr> replicationCardFuture,
        NNative::IConnectionPtr connection)
    {
        return GetTabletCount(
                std::move(replicationCardFuture),
                std::move(connection))
            .ApplyUnique(BIND([] (TErrorOr<int>&& count) {
                if (!count.IsOK()) {
                    return BuildYsonStringFluently()
                        .Entity();
                }

                return BuildYsonStringFluently()
                    .Value(count.Value());
            }));
    }

    static TFuture<int> GetTabletCount(
        TFuture<TReplicationCardPtr> replicationCardFuture,
        NNative::IConnectionPtr connection)
    {
        auto getTableInfoWaitTimeout = connection->GetConfig()->DefaulChaosReplicatedTableGetTabletCountTimeout;
        auto activeQueueConnectionsFuture = replicationCardFuture.ApplyUnique(BIND(
            [connection = std::move(connection)] (TReplicationCardPtr&& card) {
                return GetActiveQueueReplicaConnections(card->Replicas, connection);
            }));

        auto replicaTabletCountRequestsFuture = activeQueueConnectionsFuture.ApplyUnique(BIND(
            [getTableInfoWaitTimeout] (std::vector<std::pair<TYPath, NNative::IConnectionPtr>>&& connections)
            {
                std::vector<TFuture<int>> requests;
                requests.reserve(connections.size());
                for (auto& [replicaPath, replicaClusterConnection] : connections) {
                    requests.push_back(GetRemoteTabletCount(
                        replicaPath,
                        std::move(replicaClusterConnection)));
                }

                return AllSetWithTimeout(
                    std::move(requests),
                    getTableInfoWaitTimeout);
            }));

        return replicaTabletCountRequestsFuture
            .ApplyUnique(BIND([] (std::vector<TErrorOr<int>>&& tabletCounts) {
                return MakeFuture(GetMinimalTabletCount(std::move(tabletCounts)));
            }));
    }

private:
    static std::vector<std::pair<TYPath, NNative::IConnectionPtr>> GetActiveQueueReplicaConnections(
        const THashMap<TReplicaId, TReplicaInfo>& replicas,
        NNative::IConnectionPtr nativeConnection)
    {
        const auto& clusterDirectory = nativeConnection->GetClusterDirectory();
        std::vector<std::pair<TYPath, NNative::IConnectionPtr>> connections;
        for (const auto& [replicaId, replica] : replicas) {
            if (replica.ContentType != ETableReplicaContentType::Queue ||
                !IsReplicaReallySync(replica.Mode, replica.State, replica.History))
            {
                continue;
            }

            auto replicaClusterConnection = clusterDirectory->FindConnection(replica.ClusterName);
            if (!replicaClusterConnection) {
                continue;
            }

            connections.emplace_back(replica.ReplicaPath, std::move(replicaClusterConnection));
        }

        return connections;
    }

    static TFuture<int> GetRemoteTabletCount(const TYPath& path, const NNative::IConnectionPtr& connection)
    {
        auto client = connection->CreateClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
        auto tableMountInfoFuture = client->GetTableMountCache()->GetTableInfo(path);
        return tableMountInfoFuture.Apply(BIND([] (const TTableMountInfoPtr& tableMountInfo) -> int {
            return std::ssize(tableMountInfo->Tablets);
        }));
    }
};

} // namespace

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

        bool isSorted = impl->IsSorted();
        bool isQueue = impl->IsQueue();
        bool isQueueConsumer = impl->IsQueueConsumer();
        bool isQueueProducer = impl->IsQueueProducer();
        bool hasNonEmptySchema = impl->HasNonEmptySchema();

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
            .SetPresent(hasNonEmptySchema && isSorted));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TreatAsQueueProducer)
            .SetWritable(true)
            .SetPresent(hasNonEmptySchema && isSorted));

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueAgentStage)
            .SetWritable(true)
            .SetRemovable(true)
            .SetPresent(hasNonEmptySchema));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueStatus)
            .SetPresent(isQueue)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueuePartitions)
            .SetPresent(isQueue)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueConsumerStatus)
            .SetPresent(isQueueConsumer)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueConsumerPartitions)
            .SetPresent(isQueueConsumer)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueProducerStatus)
            .SetPresent(isQueueProducer)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueProducerPartitions)
            .SetPresent(isQueueProducer)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CollocatedReplicationCardIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCount)
            .SetPresent(isQueue)
            .SetOpaque(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        bool hasNonEmptySchema = node->HasNonEmptySchema();
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
                    .Value(node->GetTreatAsQueueConsumer());
                return true;
            }

            case EInternedAttributeKey::TreatAsQueueProducer: {
                if (!node->HasNonEmptySchema() || !node->IsSorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetTreatAsQueueProducer());
                return true;
            }

            case EInternedAttributeKey::QueueAgentStage:
                if (!hasNonEmptySchema) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(GetEffectiveQueueAgentStage(Bootstrap_, node->GetQueueAgentStage()));
                return true;

            default:
                break;
        }

        return TCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* table = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ChaosCellBundle: {
                ValidateNoTransaction();

                auto name = ConvertTo<std::string>(value);

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
                bool isQueueConsumerObjectBefore = lockedTableNode->IsTrackedQueueConsumerObject();
                lockedTableNode->SetTreatAsQueueConsumer(ConvertTo<bool>(value));
                bool isQueueConsumerObjectAfter = lockedTableNode->IsTrackedQueueConsumerObject();
                const auto& chaosManager = Bootstrap_->GetChaosManager();
                if (isQueueConsumerObjectAfter && !isQueueConsumerObjectBefore) {
                    chaosManager->RegisterQueueConsumer(lockedTableNode);
                } else if (!isQueueConsumerObjectAfter && isQueueConsumerObjectBefore) {
                    chaosManager->UnregisterQueueConsumer(lockedTableNode);
                }

                SetModified(EModificationType::Attributes);

                return true;
            }

            case EInternedAttributeKey::TreatAsQueueProducer: {
                ValidateNoTransaction();
                auto* lockedTableNode = LockThisImpl();
                if (!lockedTableNode->HasNonEmptySchema() || !lockedTableNode->IsSorted()) {
                    break;
                }
                bool isQueueProducerObjectBefore = lockedTableNode->IsTrackedQueueProducerObject();
                lockedTableNode->SetTreatAsQueueProducer(ConvertTo<bool>(value));
                bool isQueueProducerObjectAfter = lockedTableNode->IsTrackedQueueProducerObject();
                const auto& chaosManager = Bootstrap_->GetChaosManager();
                if (isQueueProducerObjectAfter && !isQueueProducerObjectBefore) {
                    chaosManager->RegisterQueueProducer(lockedTableNode);
                } else if (!isQueueProducerObjectAfter && isQueueProducerObjectBefore) {
                    chaosManager->UnregisterQueueProducer(lockedTableNode);
                }

                SetModified(EModificationType::Attributes);

                return true;
            }

            case EInternedAttributeKey::QueueAgentStage: {
                ValidateNoTransaction();

                if (!table->HasNonEmptySchema()) {
                    break;
                }

                auto* lockedTable = LockThisImpl();
                lockedTable->SetQueueAgentStage(ConvertTo<std::string>(value));

                SetModified(EModificationType::Attributes);

                return true;
            }

            default:
                break;
        }

        return TCypressNodeProxyBase::SetBuiltinAttribute(key, value, force);
    }

    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::QueueAgentStage: {
                ValidateNoTransaction();
                auto* lockedTable = LockThisImpl();
                lockedTable->SetQueueAgentStage(std::nullopt);
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* table = GetThisImpl();
        bool isQueue = table->IsQueue();
        bool isQueueConsumer = table->IsQueueConsumer();
        bool isQueueProducer = table->IsQueueProducer();

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
                    .IncludeHistory = true,
                    .IncludeReplicatedTableOptions = true,
                };

                return GetReplicationCard(options)
                    .Apply(BIND([=] (const TReplicationCardPtr& card) {
                        auto replicasLags = ComputeReplicasLag(card->Replicas);
                        return BuildYsonStringFluently()
                            .DoMapFor(replicasLags, [&] (TFluentMap fluent, const auto& lagPair) {
                                const auto& [replicaId, replicaLag] = lagPair;
                                const auto& replicas = card->Replicas;
                                const auto& replica = replicas.find(replicaId)->second;
                                auto minTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
                                fluent
                                    .Item(ToString(replicaId))
                                    .BeginMap()
                                        .Item("cluster_name").Value(replica.ClusterName)
                                        .Item("replica_path").Value(replica.ReplicaPath)
                                        .Item("state").Value(replica.State)
                                        .Item("mode").Value(replica.Mode)
                                        .Item("content_type").Value(replica.ContentType)
                                        .Item("replication_lag_timestamp").Value(minTimestamp)
                                        .Item("replication_lag_time").Value(replicaLag)
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

            case EInternedAttributeKey::CollocatedReplicationCardIds: {
                auto id = GetThisImpl()->GetReplicationCardId();
                auto connection = Bootstrap_->GetClusterConnection();
                return GetReplicationCard()
                    .ApplyUnique(BIND([connection = std::move(connection), id] (TReplicationCardPtr&& card) {
                        if (card->ReplicationCardCollocationId.IsEmpty()) {
                            return MakeFuture(
                                BuildYsonStringFluently()
                                    .BeginList()
                                    .EndList());
                        }

                        return GetCollocatedReplicationCards(
                            card->ReplicationCardCollocationId,
                            id,
                            std::move(connection))
                            .ApplyUnique(BIND([] (std::vector<TReplicationCardId>&& ids) {
                                return BuildYsonStringFluently()
                                    .Value(ids);
                            }));
                    }));
            }

            case EInternedAttributeKey::TabletCount: {
                if (!isQueue) {
                    break;
                }

                // TODO(osidorkin): Write better implementation after replication card progress format is changed (YT-17817)
                return TChaosReplicatedTableTabletsCountGetter::GetTabletsCountYson(
                    GetReplicationCard({.IncludeHistory = true}),
                    Bootstrap_->GetClusterConnection());
            }

            case EInternedAttributeKey::QueueStatus:
            case EInternedAttributeKey::QueuePartitions: {
                if (!isQueue) {
                    break;
                }
                return GetQueueAgentAttributeAsync(Bootstrap_, table->GetQueueAgentStage(), GetPath(), key);
            }

            case EInternedAttributeKey::QueueConsumerStatus:
            case EInternedAttributeKey::QueueConsumerPartitions: {
                if (!isQueueConsumer) {
                    break;
                }
                return GetQueueAgentAttributeAsync(Bootstrap_, table->GetQueueAgentStage(), GetPath(), key);
            }

            case EInternedAttributeKey::QueueProducerStatus:
            case EInternedAttributeKey::QueueProducerPartitions: {
                if (!isQueueProducer) {
                    break;
                }
                return GetQueueAgentAttributeAsync(Bootstrap_, table->GetQueueAgentStage(), GetPath(), key);
            }

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

    static TFuture<std::vector<TReplicationCardId>> GetCollocatedReplicationCards(
        TReplicationCardCollocationId collocationId,
        TReplicationCardId replicationCardId,
        NNative::IConnectionPtr connection)
    {
        auto proxy = TChaosNodeServiceProxy(connection->GetChaosChannelByCardId(replicationCardId));
        auto req = proxy.GetReplicationCardCollocation();
        ToProto(req->mutable_replication_card_collocation_id(), collocationId);
        return req->Invoke()
            .ApplyUnique(BIND([] (TChaosNodeServiceProxy::TErrorOrRspGetReplicationCardCollocationPtr&& result) {
                if (!result.IsOK()) {
                    return TErrorOr<std::vector<TReplicationCardId>>(TError(result));
                }

                return TErrorOr(
                    FromProto<std::vector<TReplicationCardId>>(result.Value()->replication_card_ids()));
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

    auto schema = trunkTable->GetSchema();
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

    if (trunkTable->IsQueue()) {
        auto tabletCountFuture = TChaosReplicatedTableTabletsCountGetter::GetTabletCount(
            GetReplicationCard({.IncludeHistory = true}),
            Bootstrap_->GetClusterConnection());

        context->ReplyFrom(tabletCountFuture.ApplyUnique(BIND(
            [context, response] (TErrorOr<int>&& result) {
                response->set_tablet_count(result.ValueOrDefault(0));
            })));
    } else {
        context->Reply();
    }
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

    if (table->IsTrackedQueueConsumerObject()) {
        bool isValidConsumerSchema = !effectiveSchema->IsEmpty() && effectiveSchema->IsSorted();
        if (!isValidConsumerSchema) {
            THROW_ERROR_EXCEPTION(
                "Chaos replicated table object cannot be both a queue and a consumer.\
                To transform consumer into queue set \"treat_as_queue_consumer\" attribute into False first");
        }
    }

    if (table->IsTrackedQueueProducerObject()) {
        bool isValidProducerSchema = !effectiveSchema->IsEmpty() && effectiveSchema->IsSorted();
        if (!isValidProducerSchema) {
            THROW_ERROR_EXCEPTION(
                "Chaos replicated table object cannot be both a queue and a producer.\
                To transform producer into queue set \"treat_as_queue_producer\" attribute into False first");
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
