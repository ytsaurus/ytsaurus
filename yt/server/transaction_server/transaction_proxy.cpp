#include "transaction_proxy.h"
#include "transaction_manager.h"
#include "transaction.h"

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cypress_server/node.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/security_server/account.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/ytree/convert.h>
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
using namespace NConcurrency;

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

        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(EInternedAttributeKey::SecondaryCellTags);
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
        descriptors->push_back(EInternedAttributeKey::ExportedObjectCount);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ImportedObjectIds)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::ImportedObjectCount);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BranchedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LockedNodeIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LockIds)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::ResourceUsage);
        descriptors->push_back(EInternedAttributeKey::MulticellResourceUsage);
        descriptors->push_back(EInternedAttributeKey::PrerequisiteTransactionIds);
        descriptors->push_back(EInternedAttributeKey::DependentTransactionIds);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Deadline)
            .SetPresent(transaction->GetDeadline().operator bool()));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(transaction->GetState());
                return true;

            case EInternedAttributeKey::SecondaryCellTags:
                BuildYsonFluently(consumer)
                    .Value(transaction->SecondaryCellTags());
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

            case EInternedAttributeKey::StagedNodeIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->StagedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                        fluent.Item().Value(node->GetId());
                    });
                return true;

            case EInternedAttributeKey::BranchedNodeIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->BranchedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                        fluent.Item().Value(node->GetId());
                    });
                return true;

            case EInternedAttributeKey::LockedNodeIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->LockedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                        fluent.Item().Value(node->GetId());
                    });
                return true;

            case EInternedAttributeKey::LockIds:
                BuildYsonFluently(consumer)
                    .DoListFor(transaction->Locks(), [=] (TFluentList fluent, const TLock* lock) {
                        fluent.Item().Value(lock->GetId());
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

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
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

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* transaction = GetThisImpl();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

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
                return GetAggregatedResourceUsageMap().Apply(BIND([=] (const TAccountResourcesMap& usageMap) {
                    return BuildYsonStringFluently()
                        .DoMapFor(usageMap, [=] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                            fluent
                                .Item(nameAndUsage.first)
                                .Value(New<TSerializableClusterResources>(chunkManager, nameAndUsage.second));
                        });
                }).AsyncVia(GetCurrentInvoker()));

            case EInternedAttributeKey::MulticellResourceUsage:
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

            case EInternedAttributeKey::StagedObjectIds: {
                return FetchMergeableAttribute(
                    GetUninternedAttributeKey(key),
                    BIND([=, this_ = MakeStrong(this)] {
                        return BuildYsonStringFluently().DoListFor(transaction->StagedObjects(), [] (TFluentList fluent, const TObjectBase* object) {
                            fluent.Item().Value(object->GetId());
                        });
                    }));
            }

            case EInternedAttributeKey::ImportedObjectCount:
                return FetchSummableAttribute(
                    GetUninternedAttributeKey(key),
                    BIND([=, this_ = MakeStrong(this)] {
                        return ConvertToYsonString(transaction->ImportedObjects().size());
                    }));

            case EInternedAttributeKey::ImportedObjectIds:
                return FetchMergeableAttribute(
                    GetUninternedAttributeKey(key),
                    BIND([=, this_ = MakeStrong(this)] {
                        return BuildYsonStringFluently().DoListFor(transaction->ImportedObjects(), [] (TFluentList fluent, const TObjectBase* object) {
                            fluent.Item().Value(object->GetId());
                        });
                    }));

            case EInternedAttributeKey::ExportedObjectCount:
                return FetchSummableAttribute(
                    GetUninternedAttributeKey(key),
                    BIND([=, this_ = MakeStrong(this)] {
                        return ConvertToYsonString(transaction->ExportedObjects().size());
                    }));

            case EInternedAttributeKey::ExportedObjects:
                return FetchMergeableAttribute(
                    GetUninternedAttributeKey(key),
                    BIND([=, this_ = MakeStrong(this)] {
                        return BuildYsonStringFluently().DoListFor(transaction->ExportedObjects(), [] (TFluentList fluent, const TTransaction::TExportEntry& entry) {
                            fluent
                                .Item().BeginMap()
                                    .Item("id").Value(entry.Object->GetId())
                                    .Item("destination_cell_tag").Value(entry.DestinationCellTag)
                                .EndMap();
                        });
                    }));

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
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            EPeerKind::LeaderOrFollower);

        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        auto transactionId = GetId();
        auto req = TYPathProxy::Get(FromObjectId(transactionId) + "/@resource_usage");
        batchReq->AddRequest(req);

        return batchReq->Invoke()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto cumulativeError = GetCumulativeError(batchRspOrError);
                if (cumulativeError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return std::make_pair(cellTag, TAccountResourcesMap());
                }

                THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError, "Error fetching resource usage of transaction %v from cell %v",
                    transactionId,
                    cellTag);

                const auto& batchRsp = batchRspOrError.Value();
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                const auto& rsp = rspOrError.Value();
                return std::make_pair(cellTag, DeserializeAccountResourcesMap(TYsonString(rsp->value())));
            }).AsyncVia(GetCurrentInvoker()));
    }

    TAccountResourcesMap DeserializeAccountResourcesMap(const TYsonString& value)
    {
        TAccountResourcesMap result;
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableAccountResources = ConvertTo<THashMap<TString, TSerializableClusterResourcesPtr>>(value);
        for (const auto& pair : serializableAccountResources) {
            result.insert(std::make_pair(pair.first, pair.second->ToClusterResources(chunkManager)));
        }
        return result;
    }


    template <class TSession>
    TFuture<void> FetchCombinedAttributeFromRemote(
        const TIntrusivePtr<TSession>& session,
        const TString& attributeKey,
        TCellTag cellTag,
        const TCallback<void(const TIntrusivePtr<TSession>& session, const TYsonString& yson)>& accumulator)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto channel = multicellManager->FindMasterChannel(cellTag, NHydra::EPeerKind::Follower);
        if (!channel) {
            return VoidFuture;
        }

        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        auto transactionId = Object_->GetId();
        auto req = TYPathProxy::Get(FromObjectId(transactionId) + "/@" + attributeKey);
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
        const TCallback<TYsonString()>& localFetcher,
        const TCallback<void(const TIntrusivePtr<TSession>& session, const TYsonString& yson)>& accumulator,
        const TCallback<TYsonString(const TIntrusivePtr<TSession>& session)>& finalizer)
    {
        auto invoker = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());

        auto session = New<TSession>();
        accumulator(session, localFetcher());

        std::vector<TFuture<void>> asyncResults;
        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
                asyncResults.push_back(FetchCombinedAttributeFromRemote(session, attributeKey, cellTag, accumulator));
            }
        }

        return Combine(asyncResults).Apply(BIND([=] {
            return finalizer(session);
        }));
    }


    TFuture<TYsonString> FetchMergeableAttribute(
        const TString& attributeKey,
        const TCallback<TYsonString()>& localFetcher)
    {
        struct TSession
            : public TIntrinsicRefCounted
        {
            THashMap<TString, TYsonString> Map;
        };

        using TSessionPtr = TIntrusivePtr<TSession>;

        return FetchCombinedAttribute<TSession>(
            attributeKey,
            BIND([=, this_ = MakeStrong(this)] () {
                return BuildYsonStringFluently()
                    .BeginMap()
                        .Item(ToString(Bootstrap_->GetCellTag())).Value(localFetcher())
                    .EndMap();
            }),
            BIND([] (const TSessionPtr& session, const TYsonString& yson) {
                auto map = ConvertTo<THashMap<TString, INodePtr>>(yson);
                for (const auto& pair : map) {
                    session->Map.emplace(pair.first, ConvertToYsonString(pair.second));
                }
            }),
            BIND([] (const TSessionPtr& session) {
                return BuildYsonStringFluently()
                    .DoMapFor(session->Map, [&] (TFluentMap fluent, const std::pair<const TString&, TYsonString>& pair) {
                        fluent.Item(pair.first).Value(pair.second);
                    });
            }));
    }

    TFuture<TYsonString> FetchSummableAttribute(
        const TString& attributeKey,
        const TCallback<TYsonString()>& localFetcher)
    {
        struct TSession
            : public TIntrinsicRefCounted
        {
            i64 Value = 0;
        };

        using TSessionPtr = TIntrusivePtr<TSession>;

        return FetchCombinedAttribute<TSession>(
            attributeKey,
            std::move(localFetcher),
            BIND([] (const TSessionPtr& session, const TYsonString& yson) {
                session->Value += ConvertTo<i64>(yson);
            }),
            BIND([] (const TSessionPtr& session) {
                return ConvertToYsonString(session->Value);
            }));

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

