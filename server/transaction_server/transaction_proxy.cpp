#include "transaction_proxy.h"
#include "transaction_manager.h"
#include "transaction.h"

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cypress_server/node.h>

#include <yt/server/security_server/account.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NHydra;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TTransactionProxy
    : public TNonversionedObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction)
        : TBase(bootstrap, metadata, transaction)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTransaction> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* transaction = GetThisImpl();

        descriptors->push_back("state");
        descriptors->push_back("secondary_cell_tags");
        descriptors->push_back(TAttributeDescriptor("timeout")
            .SetPresent(transaction->GetTimeout().HasValue())
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("last_ping_time")
            .SetPresent(transaction->GetTimeout().HasValue()));
        descriptors->push_back(TAttributeDescriptor("title")
            .SetPresent(transaction->GetTitle().HasValue()));
        descriptors->push_back("accounting_enabled");
        descriptors->push_back(TAttributeDescriptor("parent_id")
            .SetReplicated(true));
        descriptors->push_back("start_time");
        descriptors->push_back(TAttributeDescriptor("nested_transaction_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("staged_object_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("exported_objects")
            .SetOpaque(true));
        descriptors->push_back("exported_object_count");
        descriptors->push_back(TAttributeDescriptor("imported_object_ids")
            .SetOpaque(true));
        descriptors->push_back("imported_object_count");
        descriptors->push_back(TAttributeDescriptor("staged_node_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("branched_node_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("locked_node_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("lock_ids")
            .SetOpaque(true));
        descriptors->push_back("resource_usage");
        descriptors->push_back("multicell_resource_usage");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisImpl();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetState());
            return true;
        }

        if (key == "secondary_cell_tags") {
            BuildYsonFluently(consumer)
                .Value(transaction->SecondaryCellTags());
            return true;
        }

        if (key == "timeout" && transaction->GetTimeout()) {
            BuildYsonFluently(consumer)
                .Value(*transaction->GetTimeout());
            return true;
        }

        if (key == "title" && transaction->GetTitle()) {
            BuildYsonFluently(consumer)
                .Value(*transaction->GetTitle());
            return true;
        }

        if (key == "accounting_enabled") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetAccountingEnabled());
            return true;
        }

        if (key == "parent_id") {
            BuildYsonFluently(consumer)
                .Value(GetObjectId(transaction->GetParent()));
            return true;
        }

        if (key == "start_time") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetStartTime());
            return true;
        }

        if (key == "nested_transaction_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->NestedTransactions(), [=] (TFluentList fluent, TTransaction* nestedTransaction) {
                    fluent.Item().Value(nestedTransaction->GetId());
                });
            return true;
        }

        if (key == "staged_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->StagedObjects(), [=] (TFluentList fluent, const TObjectBase* object) {
                    fluent.Item().Value(object->GetId());
                });
            return true;
        }

        if (key == "exported_objects") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->ExportedObjects(), [=] (TFluentList fluent, const TTransaction::TExportEntry& entry) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(entry.Object->GetId())
                            .Item("destination_cell_tag").Value(entry.DestinationCellTag)
                        .EndMap();
                });
            return true;
        }

        if (key == "exported_object_count") {
            BuildYsonFluently(consumer)
                .Value(transaction->ExportedObjects().size());
            return true;
        }

        if (key == "imported_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->ImportedObjects(), [=] (TFluentList fluent, const TObjectBase* object) {
                    fluent.Item().Value(object->GetId());
                });
            return true;
        }

        if (key == "imported_object_count") {
            BuildYsonFluently(consumer)
                .Value(transaction->ImportedObjects().size());
            return true;
        }

        if (key == "staged_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->StagedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
                });
            return true;
        }

        if (key == "branched_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->BranchedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
                });
            return true;
        }

        if (key == "locked_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->LockedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
                });
            return true;
        }

        if (key == "lock_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->Locks(), [=] (TFluentList fluent, const TLock* lock) {
                    fluent.Item().Value(lock->GetId());
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        const auto* transaction = GetThisImpl();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        if (key == "last_ping_time") {
            RequireLeader();
            return Bootstrap_
                ->GetTransactionManager()
                ->GetLastPingTime(transaction)
                .Apply(BIND([] (TInstant value) {
                    return ConvertToYsonString(value);
                }));
        }

        if (key == "resource_usage") {
            return GetAggregatedResourceUsageMap().Apply(BIND([=] (const TAccountResourcesMap& usageMap) {
                return BuildYsonStringFluently()
                    .DoMapFor(usageMap, [=] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                        fluent
                            .Item(nameAndUsage.first)
                            .Value(New<TSerializableClusterResources>(chunkManager, nameAndUsage.second));
                    });
            }).AsyncVia(GetCurrentInvoker()));
        }

        if (key == "multicell_resource_usage") {
            return GetMulticellResourceUsageMap().Apply(BIND([=] (const TMulticellAccountResourcesMap& multicellUsageMap) {
                return BuildYsonStringFluently()
                    .DoMapFor(multicellUsageMap, [=] (TFluentMap fluent, const TMulticellAccountResourcesMap::value_type& cellTagAndUsageMap) {
                        fluent
                            .Item(ToString(cellTagAndUsageMap.first))
                            .DoMapFor(cellTagAndUsageMap.second, [=] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                                fluent
                                    .Item(nameAndUsage.first)
                                    .Value(New<TSerializableClusterResources>(chunkManager, nameAndUsage.second));
                            });
                    });
            }).AsyncVia(GetCurrentInvoker()));
        }

        return Null;
    }

    // account name -> cluster resources
    using TAccountResourcesMap = yhash<Stroka, NSecurityServer::TClusterResources>;
    // cell tag -> account name -> cluster resources
    using TMulticellAccountResourcesMap = yhash_map<TCellTag, TAccountResourcesMap>;

    TFuture<TMulticellAccountResourcesMap> GetMulticellResourceUsageMap()
    {
        std::vector<TFuture<std::pair<TCellTag, TAccountResourcesMap>>> asyncResults;
        asyncResults.push_back(GetLocalResourcesMap(Bootstrap_->GetCellTag()));
        if (Bootstrap_->IsPrimaryMaster()) {
            for (auto cellTag : Bootstrap_->GetSecondaryCellTags()) {
                asyncResults.push_back(GetRemoteResourcesMap(cellTag));
            }
        }

        return Combine(asyncResults).Apply(BIND([] (const std::vector<std::pair<TCellTag, TAccountResourcesMap>>& results) {
            TMulticellAccountResourcesMap multicellMap;
            for (const auto& pair : results) {
                YCHECK(multicellMap.insert(pair).second);
            }
            return multicellMap;
        }));
    }

    TFuture<TAccountResourcesMap> GetAggregatedResourceUsageMap()
    {
        return GetMulticellResourceUsageMap().Apply(BIND([] (const TMulticellAccountResourcesMap& multicellMap) {
            TAccountResourcesMap aggregatedMap;
            for (const auto& cellTagAndUsageMap : multicellMap) {
                for (const auto& nameAndUsage : cellTagAndUsageMap.second) {
                    aggregatedMap[nameAndUsage.first] += nameAndUsage.second;
                }
            }
            return aggregatedMap;
        }));
    }

    TFuture<std::pair<TCellTag, TAccountResourcesMap>> GetLocalResourcesMap(TCellTag cellTag)
    {
        const auto* transaction = GetThisImpl();
        TAccountResourcesMap result;
        for (const auto& pair : transaction->AccountResourceUsage()) {
            YCHECK(result.insert(std::make_pair(pair.first->GetName(), pair.second)).second);
        }
        return MakeFuture(std::make_pair(cellTag, result));
    }

    TFuture<std::pair<TCellTag, TAccountResourcesMap>> GetRemoteResourcesMap(TCellTag cellTag)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            EPeerKind::LeaderOrFollower);

        auto id = GetId();
        auto req = TYPathProxy::Get(FromObjectId(id) + "/@resource_usage");

        TObjectServiceProxy proxy(channel);
        return proxy.Execute(req).Apply(BIND([=] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
            if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                // Transaction is missing.
                return std::make_pair(cellTag, TAccountResourcesMap());
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching resource usage of transaction %v from cell %v",
                id,
                cellTag);
            const auto& rsp = rspOrError.Value();

            return std::make_pair(
                cellTag,
                DeserializeAccountResourcesMap(TYsonString(rsp->value()), chunkManager));
        }).AsyncVia(GetCurrentInvoker()));
    }

    static TAccountResourcesMap DeserializeAccountResourcesMap(const TYsonString& value, const NChunkServer::TChunkManagerPtr& chunkManager)
    {
        using TSerializableAccountResourcesMap = yhash<Stroka, TSerializableClusterResourcesPtr>;

        auto serializableAccountResources = ConvertTo<TSerializableAccountResourcesMap>(value);

        TAccountResourcesMap result;
        for (const auto& pair : serializableAccountResources) {
            result.insert(std::make_pair(pair.first, pair.second->ToClusterResources(chunkManager)));
        }

        return result;
    }
};

IObjectProxyPtr CreateTransactionProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction)
{
    return New<TTransactionProxy>(bootstrap, metadata, transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

