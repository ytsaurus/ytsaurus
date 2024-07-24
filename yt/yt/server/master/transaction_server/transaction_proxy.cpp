#include "transaction_proxy.h"

#include "transaction_manager.h"
#include "transaction.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/account_resource_usage_lease.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTransactionServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTransactionProxy
    : public TNonversionedObjectProxyBase<TTransaction>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TTransaction>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* transaction = GetThisImpl();

        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(EInternedAttributeKey::ReplicatedToCellTags);
        descriptors->push_back(EInternedAttributeKey::ExternalizedToCellTags);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Timeout)
            .SetPresent(transaction->GetTimeout().operator bool())
            .SetWritable(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastPingTime)
            .SetPresent(transaction->GetTimeout().operator bool()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Title)
            .SetPresent(transaction->GetTitle().operator bool()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentId)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::StartTime);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NestedTransactionIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagedObjectIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExportedObjects)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExportedObjectCount)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalExportedObjectCount)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ImportedObjectIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ImportedObjectCount)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BranchedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LockedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LockIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveLockCount)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceUsage)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellResourceUsage)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AccountResourceUsageLeaseIds)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::PrerequisiteTransactionIds);
        descriptors->push_back(EInternedAttributeKey::DependentTransactionIds);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Deadline)
            .SetPresent(transaction->GetDeadline().operator bool()));
        descriptors->push_back(EInternedAttributeKey::Depth);
        descriptors->push_back(EInternedAttributeKey::CypressTransaction);
        descriptors->push_back(EInternedAttributeKey::LeaseCellIds);
        descriptors->push_back(EInternedAttributeKey::SuccessorTransactionLeaseCount);
        descriptors->push_back(EInternedAttributeKey::LeasesState);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetPersistentState());
                return true;

            case EInternedAttributeKey::ReplicatedToCellTags:
                BuildYsonFluently(consumer)
                    .Value(transaction->ReplicatedToCellTags());
                return true;

            case EInternedAttributeKey::ExternalizedToCellTags:
                BuildYsonFluently(consumer)
                    .Value(transaction->ExternalizedToCellTags());
                return true;

            case EInternedAttributeKey::Timeout:
                if (!transaction->GetTimeout()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(*transaction->GetTimeout());
                return true;

            case EInternedAttributeKey::Title:
                if (!transaction->GetTitle()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(*transaction->GetTitle());
                return true;

            case EInternedAttributeKey::ParentId:
                BuildYsonFluently(consumer)
                    .Value(GetObjectId(transaction->GetParent()));
                return true;

            case EInternedAttributeKey::StartTime:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetStartTime());
                return true;

            case EInternedAttributeKey::NestedTransactionIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->NestedTransactions(), [=] (TFluentList fluent, TTransaction* nestedTransaction) {
                        fluent.Item().Value(nestedTransaction->GetId());
                    });
                return true;

            case EInternedAttributeKey::PrerequisiteTransactionIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->PrerequisiteTransactions(), [=] (auto fluent, const auto* transaction) {
                        fluent.Item().Value(transaction->GetId());
                    });
                return true;

            case EInternedAttributeKey::DependentTransactionIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->DependentTransactions(), [=] (auto fluent, const auto* transaction) {
                        fluent.Item().Value(transaction->GetId());
                    });
                return true;

            case EInternedAttributeKey::Deadline:
                if (!transaction->GetDeadline()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(transaction->GetDeadline());
                return true;

            case EInternedAttributeKey::Depth:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetDepth());
                return true;

            case EInternedAttributeKey::AccountResourceUsageLeaseIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->AccountResourceUsageLeases(), [=] (auto fluent, const auto* accountResourceUsageLease) {
                        fluent.Item().Value(accountResourceUsageLease->GetId());
                    });
                return true;

            case EInternedAttributeKey::CypressTransaction:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetIsCypressTransaction());
                return true;

            case EInternedAttributeKey::LeaseCellIds:
                BuildYsonFluently(consumer)
                    .Value(transaction->LeaseCellIds());
                return true;

            case EInternedAttributeKey::SuccessorTransactionLeaseCount:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetSuccessorTransactionLeaseCount());
                return true;

            case EInternedAttributeKey::LeasesState:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetTransactionLeasesState());
                return true;

            case EInternedAttributeKey::LocalExportedObjectCount:
                BuildYsonFluently(consumer)
                    .Value(transaction->ExportedObjects().size());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* transaction = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Timeout:
                Bootstrap_
                    ->GetTransactionManager()
                    ->SetTransactionTimeout(
                        transaction,
                        TDuration::MilliSeconds(ConvertTo<i64>(value)));
                return true;
            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* transaction = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::LastPingTime:
                RequireLeader();
                return Bootstrap_
                    ->GetTransactionManager()
                    ->GetLastPingTime(transaction)
                    .Apply(BIND([] (TInstant value) {
                        return ConvertToYsonString(value);
                    }));

            case EInternedAttributeKey::ResourceUsage:
                return GetAggregatedResourceUsageMap().Apply(BIND([=, this, this_ = MakeStrong(this)] (const TAccountResourcesMap& usageMap) {
                    return BuildYsonStringFluently()
                        .DoMapFor(usageMap, [&] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                            fluent
                                .Item(nameAndUsage.first);
                            SerializeClusterResources(nameAndUsage.second, fluent.GetConsumer(), Bootstrap_);
                        });
                }).AsyncVia(GetCurrentInvoker()));

            case EInternedAttributeKey::MulticellResourceUsage:
                return GetMulticellResourceUsageMap().Apply(BIND([=, this, this_ = MakeStrong(this)] (const TMulticellAccountResourcesMap& multicellUsageMap) {
                    return BuildYsonStringFluently()
                        .DoMapFor(multicellUsageMap, [&] (TFluentMap fluent, const TMulticellAccountResourcesMap::value_type& cellTagAndUsageMap) {
                            fluent
                                .Item(ToString(cellTagAndUsageMap.first))
                                .DoMapFor(cellTagAndUsageMap.second, [&] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                                    fluent
                                        .Item(nameAndUsage.first);
                                    SerializeClusterResources(nameAndUsage.second, fluent.GetConsumer(), Bootstrap_);
                                });
                        });
                }).AsyncVia(GetCurrentInvoker()));

            case EInternedAttributeKey::StagedNodeIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->StagedNodes(), [] (TFluentList fluent, const TCypressNode* node) {
                            fluent.Item().Value(node->GetId());
                        });
                    });

            case EInternedAttributeKey::BranchedNodeIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->BranchedNodes(), [] (TFluentList fluent, const TCypressNode* node) {
                            fluent.Item().Value(node->GetId());
                        });
                    });

            case EInternedAttributeKey::LockedNodeIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->LockedNodes(), [] (TFluentList fluent, const TCypressNode* node) {
                            fluent.Item().Value(node->GetId());
                        });
                    });

            case EInternedAttributeKey::LockIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->Locks(), [] (TFluentList fluent, const TLock* lock) {
                            fluent.Item().Value(lock->GetId());
                        });
                    });

            case EInternedAttributeKey::RecursiveLockCount:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return ConvertToYsonString(transaction->GetRecursiveLockCount());
                    });

            case EInternedAttributeKey::StagedObjectIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->StagedObjects(), [] (TFluentList fluent, const TObject* object) {
                            fluent.Item().Value(object->GetId());
                        });
                    });

            case EInternedAttributeKey::ImportedObjectCount:
                return FetchSummableAttribute(
                    key.Unintern(),
                    [&] {
                        return ConvertToYsonString(transaction->ImportedObjects().size());
                    });

            case EInternedAttributeKey::ImportedObjectIds:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->ImportedObjects(), [] (TFluentList fluent, const TObject* object) {
                            fluent.Item().Value(object->GetId());
                        });
                    });

            case EInternedAttributeKey::ExportedObjectCount:
                return FetchSummableAttribute(
                    key.Unintern(),
                    [&] {
                        return ConvertToYsonString(transaction->ExportedObjects().size());
                    });

            case EInternedAttributeKey::ExportedObjects:
                return FetchMergeableAttribute(
                    key.Unintern(),
                    [&] {
                        return BuildYsonStringFluently().DoListFor(transaction->ExportedObjects(), [] (TFluentList fluent, const TTransaction::TExportEntry& entry) {
                            fluent
                                .Item().BeginMap()
                                    .Item("id").Value(entry.Object->GetId())
                                    .Item("destination_cell_tag").Value(entry.DestinationCellTag)
                                .EndMap();
                        });
                    });

            default:
                break;
        }

        return std::nullopt;
    }

    // Account name -> cluster resources.
    using TAccountResourcesMap = THashMap<TString, NSecurityServer::TClusterResources>;
    // Cell tag -> account name -> cluster resources.
    using TMulticellAccountResourcesMap = THashMap<TCellTag, TAccountResourcesMap>;

    TFuture<TMulticellAccountResourcesMap> GetMulticellResourceUsageMap()
    {
        const auto* transaction = GetThisImpl();

        std::vector<TFuture<std::pair<TCellTag, TAccountResourcesMap>>> asyncResults;
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        asyncResults.push_back(GetLocalResourcesMap(multicellManager->GetCellTag()));
        if (transaction->IsNative()) {
            for (auto cellTag : transaction->ReplicatedToCellTags()) {
                asyncResults.push_back(GetRemoteResourcesMap(cellTag));
            }
        }

        return AllSucceeded(asyncResults).Apply(BIND([] (const std::vector<std::pair<TCellTag, TAccountResourcesMap>>& results) {
            TMulticellAccountResourcesMap multicellMap;
            for (const auto& pair : results) {
                YT_VERIFY(multicellMap.insert(pair).second);
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
        for (const auto& [account, resources] : transaction->AccountResourceUsage()) {
            YT_VERIFY(result.emplace(account->GetName(), resources).second);
        }
        return MakeFuture(std::pair(cellTag, result));
    }

    TFuture<std::pair<TCellTag, TAccountResourcesMap>> GetRemoteResourcesMap(TCellTag cellTag)
    {
        auto proxy = CreateObjectServiceReadProxy(
            Bootstrap_->GetRootClient(),
            NApi::EMasterChannelKind::Follower,
            cellTag);
        auto batchReq = proxy.ExecuteBatch();

        auto transactionId = GetId();
        auto req = TYPathProxy::Get("&" + FromObjectId(transactionId) + "/@resource_usage");
        batchReq->AddRequest(req);

        return batchReq->Invoke()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto cumulativeError = GetCumulativeError(batchRspOrError);
                if (cumulativeError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return std::pair(cellTag, TAccountResourcesMap());
                }

                THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError, "Error fetching resource usage of transaction %v from cell %v",
                    transactionId,
                    cellTag);

                const auto& batchRsp = batchRspOrError.Value();
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                const auto& rsp = rspOrError.Value();
                return std::pair(cellTag, DeserializeAccountResourcesMap(TYsonString(rsp->value())));
            }).AsyncVia(GetCurrentInvoker()));
    }

    TAccountResourcesMap DeserializeAccountResourcesMap(const TYsonString& value)
    {
        TAccountResourcesMap result;
        auto map = ConvertToNode(value)->AsMap();
        for (const auto& [accountName, resources] : map->GetChildren()) {
            DeserializeClusterResources(result[accountName], resources, Bootstrap_);
        }
        return result;
    }

    template <class TSession>
    TFuture<void> FetchCombinedAttributeFromRemote(
        const TIntrusivePtr<TSession>& session,
        const TString& attributeKey,
        TCellTag cellTag,
        const std::function<void(const TIntrusivePtr<TSession>& session, const TYsonString& yson)>& accumulator)
    {
        auto proxy = CreateObjectServiceReadProxy(
            Bootstrap_->GetRootClient(),
            NApi::EMasterChannelKind::Follower,
            cellTag);
        auto batchReq = proxy.ExecuteBatch();

        auto transactionId = Object_->GetId();
        auto req = TYPathProxy::Get("&" + FromObjectId(transactionId) + "/@" + attributeKey);
        batchReq->AddRequest(req);

        return batchReq->Invoke()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto cumulativeError = GetCumulativeError(batchRspOrError);
                if (cumulativeError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return;
                }

                THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError, "Error fetching attribute %Qv of transaction %v from cell %v",
                    attributeKey,
                    transactionId,
                    cellTag);

                const auto& batchRsp = batchRspOrError.Value();
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                const auto& rsp = rspOrError.Value();
                accumulator(session, TYsonString(rsp->value()));
            }).AsyncVia(GetCurrentInvoker()));
    }

    template <class TSession>
    TFuture<TYsonString> FetchCombinedAttribute(
        const TString& attributeKey,
        const std::function<TYsonString()>& localFetcher,
        const std::function<void(const TIntrusivePtr<TSession>& session, const TYsonString& yson)>& accumulator,
        const std::function<TYsonString(const TIntrusivePtr<TSession>& session)>& finalizer)
    {
        auto invoker = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());

        auto session = New<TSession>();
        accumulator(session, localFetcher());

        const auto* transaction = GetThisImpl();

        std::vector<TFuture<void>> asyncResults;
        if (transaction->IsNative()) {
            for (auto cellTag : transaction->ReplicatedToCellTags()) {
                asyncResults.push_back(FetchCombinedAttributeFromRemote(session, attributeKey, cellTag, accumulator));
            }
        }

        return AllSucceeded(asyncResults).Apply(BIND([=, this_ = MakeStrong(this)] {
            return finalizer(session);
        }));
    }


    TFuture<TYsonString> FetchMergeableAttribute(
        const TString& attributeKey,
        const std::function<TYsonString()>& localFetcher)
    {
        struct TSession
            : public TRefCounted
        {
            THashMap<TString, TYsonString> Map;
        };

        using TSessionPtr = TIntrusivePtr<TSession>;

        return FetchCombinedAttribute<TSession>(
            attributeKey,
            [&] {
                return BuildYsonStringFluently()
                    .BeginMap()
                        .Item(ToString(Bootstrap_->GetMulticellManager()->GetCellTag())).Value(localFetcher())
                    .EndMap();
            },
            [] (const TSessionPtr& session, const TYsonString& yson) {
                auto map = ConvertTo<THashMap<TString, INodePtr>>(yson);
                for (const auto& [key, value] : map) {
                    session->Map.emplace(key, ConvertToYsonString(value));
                }
            },
            [] (const TSessionPtr& session) {
                return BuildYsonStringFluently()
                    .DoMapFor(session->Map, [&] (TFluentMap fluent, const std::pair<const TString&, TYsonString>& pair) {
                        fluent.Item(pair.first).Value(pair.second);
                    });
            });
    }

    TFuture<TYsonString> FetchSummableAttribute(
        const TString& attributeKey,
        const std::function<TYsonString()>& localFetcher)
    {
        struct TSession
            : public TRefCounted
        {
            i64 Value = 0;
        };

        using TSessionPtr = TIntrusivePtr<TSession>;

        return FetchCombinedAttribute<TSession>(
            attributeKey,
            localFetcher,
            [] (const TSessionPtr& session, const TYsonString& yson) {
                session->Value += ConvertTo<i64>(yson);
            },
            [] (const TSessionPtr& session) {
                return ConvertToYsonString(session->Value);
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTransactionProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction)
{
    return New<TTransactionProxy>(bootstrap, metadata, transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer

