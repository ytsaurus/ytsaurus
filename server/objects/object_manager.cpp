#include "object_manager.h"

#include "account_type_handler.h"
#include "config.h"
#include "db_schema.h"
#include "dns_record_set_type_handler.h"
#include "dynamic_resource_type_handler.h"
#include "endpoint_set_type_handler.h"
#include "endpoint_type_handler.h"
#include "group_type_handler.h"
#include "internet_address_type_handler.h"
#include "ip4_address_pool_type_handler.h"
#include "multi_cluster_replica_set_type_handler.h"
#include "network_project_type_handler.h"
#include "node_segment_type_handler.h"
#include "node_type_handler.h"
#include "object.h"
#include "pod_disruption_budget_type_handler.h"
#include "pod_set_type_handler.h"
#include "pod_type_handler.h"
#include "private.h"
#include "project_type_handler.h"
#include "replica_set_type_handler.h"
#include "resource_cache_type_handler.h"
#include "resource_type_handler.h"
#include "schema_type_handler.h"
#include "stage_type_handler.h"
#include "table_version_checker.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "type_handler.h"
#include "user_type_handler.h"
#include "virtual_service_type_handler.h"
#include "watch_manager.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/lib/objects/type_info.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/rowset.h>

#include <yt/client/table_client/helpers.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYP::NServer::NObjects {

using namespace NServer::NMaster;

using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NCypressClient;
using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TObjectManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , SweepExecutor_(New<TPeriodicExecutor>(
            SweepQueue_->GetInvoker(),
            BIND(&TImpl::OnSweep, MakeWeak(this)),
            Config_->RemovedObjectsSweepPeriod))
    { }

    void Initialize()
    {
        RegisterTypeHandler(CreateSchemaTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateNodeTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateResourceTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreatePodTypeHandler(Bootstrap_, Config_->PodTypeHandler));
        RegisterTypeHandler(CreatePodSetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateEndpointTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateEndpointSetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateStageTypeHandler(Bootstrap_, Config_->PodTypeHandler->SpecValidation));
        RegisterTypeHandler(CreateNetworkProjectTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateNodeSegmentTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateVirtualServiceTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateUserTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateGroupTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateInternetAddressTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateIP4AddressPoolsTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateAccountTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateReplicaSetTypeHandler(Bootstrap_, Config_->PodTypeHandler->SpecValidation));
        RegisterTypeHandler(CreateDnsRecordSetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateResourceCacheTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateMultiClusterReplicaSetTypeHandler(Bootstrap_, Config_->PodTypeHandler->SpecValidation));
        RegisterTypeHandler(CreateDynamicResourceTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreatePodDisruptionBudgetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateProjectTypeHandler(Bootstrap_));

        for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
            if (auto typeHandler = FindTypeHandler(type)) {
                typeHandler->Initialize();
                typeHandler->PostInitialize();
            }
        }

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

    IObjectTypeHandler* GetTypeHandler(EObjectType type)
    {
        auto* typeHandler = FindTypeHandler(type);
        YT_VERIFY(typeHandler);
        return typeHandler;
    }

    IObjectTypeHandler* GetTypeHandlerOrThrow(EObjectType type)
    {
        auto* typeHandler = FindTypeHandler(type);
        if (!typeHandler) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectType,
                "Unknown object type %v",
                type);
        }
        return typeHandler;
    }

    IObjectTypeHandler* FindTypeHandler(EObjectType type)
    {
        return type >= TEnumTraits<EObjectType>::GetMinValue() && type <= TEnumTraits<EObjectType>::GetMaxValue()
            ? TypeHandlers_[type].get()
            : nullptr;
    }


    bool AreExtensibleAttributesEnabled() const
    {
        return Config_->EnableExtensibleAttributes;
    }

    bool IsHistoryEnabled() const
    {
        return Config_->EnableHistory;
    }

private:
    TBootstrap* const Bootstrap_;
    const TObjectManagerConfigPtr Config_;

    const TActionQueuePtr SweepQueue_ = New<TActionQueue>("Sweep");
    const TPeriodicExecutorPtr SweepExecutor_;

    TEnumIndexedVector<EObjectType, std::unique_ptr<IObjectTypeHandler>> TypeHandlers_;

private:
    void RegisterTypeHandler(std::unique_ptr<IObjectTypeHandler> handler)
    {
        auto type = handler->GetType();
        YT_VERIFY(!TypeHandlers_[type]);
        Bootstrap_->GetWatchManager()->RegisterWatchableObjectType(type, handler->GetTable()->Name);
        TypeHandlers_[type] = std::move(handler);
    }

    void OnValidateConnection()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto transaction = ytConnector->GetInstanceLockTransaction();

        {
            const auto& path = ytConnector->GetDBPath();
            YT_LOG_INFO("Locking DB (Path: %v)",
                path);

            WaitFor(transaction->LockNode(path, ELockMode::Shared))
                .ThrowOnError();
        }

        TTableVersionChecker tableVersionChecker(Bootstrap_->GetYTConnector());
        for (const auto* table : Tables) {
            tableVersionChecker.ScheduleCheck(table);
        }
        tableVersionChecker.Check();
    }


    void OnStartedLeading()
    {
        SweepExecutor_->Start();
    }

    void OnStoppedLeading()
    {
        SweepExecutor_->Stop();
    }

    void OnSweep()
    {
        try {
            for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
                auto* typeHandler = FindTypeHandler(type);
                if (!typeHandler) {
                    continue;
                }

                SweepTable(typeHandler->GetTable(), &ObjectsTable.Fields.Meta_RemovalTime);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to perform removed objects sweep");
        }
    }

    void SweepTable(
        const TDBTable* table,
        const TDBField* removalTimeField)
    {
        auto rowset = FindDeadRows(table, removalTimeField);
        if (!rowset->GetRows().Empty()) {
            DropDeadRows(table, rowset);
        }
    }

    TString BuildSweepQuery(
        const TDBTable* table,
        const TDBField* removalTimeField,
        TInstant deadline)
    {
        TStringBuilder builder;
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        builder.AppendFormat("%v from %v where not is_null(%v) and %v < %vu",
            JoinToString(table->Key, [] (auto* builder, const auto* field) {
                builder->AppendFormat("%v",
                    FormatId(field->Name));
            }),
            FormatId(ytConnector->GetTablePath(table)),
            FormatId(removalTimeField->Name),
            FormatId(removalTimeField->Name),
            deadline.MicroSeconds());
        return builder.Flush();
    }

    IUnversionedRowsetPtr FindDeadRows(
        const TDBTable* table,
        const TDBField* removalTimeField)
    {
        auto deadline = TInstant::Now() - Config_->RemovedObjectsGraceTimeout;
        YT_LOG_INFO("Selecting dead rows (Table: %v, Deadline: %v)",
            table->Name,
            deadline);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
            .ValueOrThrow();

        IUnversionedRowsetPtr result;
        auto* session = transaction->GetSession();
        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                TStringBuilder queryBuilder;
                context->ScheduleSelect(
                    BuildSweepQuery(table, removalTimeField, deadline),
                    [&] (const auto& rowset) mutable {
                        result = rowset;
                    });
            });
        session->FlushLoads();

        YT_LOG_INFO("Dead rows selected (Count: %v)",
            result->GetRows().Size());

        return result;
    }

    void DropDeadRows(
        const TDBTable* table,
        const IUnversionedRowsetPtr& rowset)
    {
        auto rows = rowset->GetRows();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        const size_t batchSize = std::max(
            std::min(static_cast<size_t>(Config_->RemovedObjectsDropBatchSize), rows.Size()),
            1ul);

        for (size_t startOffset = 0; startOffset < rows.Size(); startOffset += batchSize) {
            auto endOffset = std::min(startOffset + batchSize, rows.Size());

            auto Logger = NLogging::TLogger(NObjects::Logger)
                .AddTag("StartOffset: %v", startOffset)
                .AddTag("EndOffset: %v", endOffset);

            YT_LOG_INFO("Starting removal transaction");

            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_INFO("Removal transaction started (TransactionId: %v)",
                transaction->GetId());

            auto* session = transaction->GetSession();
            session->ScheduleStore(
                [&, startOffset, endOffset] (IStoreContext* context) {
                    for (auto row : rows.Slice(startOffset, endOffset)) {
                        context->DeleteRow(
                            table,
                            TRange<TUnversionedValue>(row.Begin(), row.End()));
                    }
                });

            YT_LOG_INFO("Committing removal transaction");

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_INFO("Removal transaction committed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(TBootstrap* bootstrap, TObjectManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void TObjectManager::Initialize()
{
    Impl_->Initialize();
}

IObjectTypeHandler* TObjectManager::GetTypeHandler(EObjectType type)
{
    return Impl_->GetTypeHandler(type);
}

IObjectTypeHandler* TObjectManager::GetTypeHandlerOrThrow(EObjectType type)
{
    return Impl_->GetTypeHandlerOrThrow(type);
}

IObjectTypeHandler* TObjectManager::FindTypeHandler(EObjectType type)
{
    return Impl_->FindTypeHandler(type);
}

bool TObjectManager::AreExtensibleAttributesEnabled() const
{
    return Impl_->AreExtensibleAttributesEnabled();
}

bool TObjectManager::IsHistoryEnabled() const
{
    return Impl_->IsHistoryEnabled();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

