#include "object_manager.h"
#include "config.h"
#include "type_handler.h"
#include "schema_type_handler.h"
#include "node_type_handler.h"
#include "resource_type_handler.h"
#include "pod_type_handler.h"
#include "pod_set_type_handler.h"
#include "endpoint_type_handler.h"
#include "endpoint_set_type_handler.h"
#include "network_project_type_handler.h"
#include "node_segment_type_handler.h"
#include "virtual_service_type_handler.h"
#include "user_type_handler.h"
#include "group_type_handler.h"
#include "internet_address_type_handler.h"
#include "object.h"
#include "db_schema.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NServer::NMaster;

using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NCypressClient;
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
        , SweepQueue_(New<TActionQueue>("Sweep"))
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
        RegisterTypeHandler(CreatePodTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreatePodSetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateEndpointTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateEndpointSetTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateNetworkProjectTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateNodeSegmentTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateVirtualServiceTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateUserTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateGroupTypeHandler(Bootstrap_));
        RegisterTypeHandler(CreateInternetAddressTypeHandler(Bootstrap_));

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

    IObjectTypeHandler* GetTypeHandler(EObjectType type)
    {
        auto* typeHandler = FindTypeHandler(type);
        YCHECK(typeHandler);
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

private:
    TBootstrap* const Bootstrap_;
    const TObjectManagerConfigPtr Config_;

    const TActionQueuePtr SweepQueue_;
    const TPeriodicExecutorPtr SweepExecutor_;

    TEnumIndexedVector<std::unique_ptr<IObjectTypeHandler>, EObjectType> TypeHandlers_;

private:
    void RegisterTypeHandler(std::unique_ptr<IObjectTypeHandler> handler)
    {
        auto type = handler->GetType();
        YCHECK(!TypeHandlers_[type]);
        TypeHandlers_[type] = std::move(handler);
    }

    void OnValidateConnection()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto transaction = ytConnector->GetInstanceLockTransaction();

        {
            const auto& path = ytConnector->GetDBPath();
            LOG_INFO("Locking DB (Path: %v)",
                path);

            WaitFor(transaction->LockNode(path, ELockMode::Shared))
                .ThrowOnError();
        }

        {
            std::vector<TFuture<void>> asyncResults;
            for (const auto* table : Tables) {
                auto path = ytConnector->GetTablePath(table);

                LOG_INFO("Checking DB table version (Path: %v)",
                    path);

                asyncResults.push_back(transaction->GetNode(path + "/@version")
                    .Apply(BIND([=] (const TErrorOr<TYsonString>& ysonVersionOrError) {
                        if (ysonVersionOrError.IsOK()) {
                            auto version = ConvertTo<int>(ysonVersionOrError.Value());
                            if (version != DBVersion) {
                                THROW_ERROR_EXCEPTION("Table %v version mismatch: expected %v, found %v",
                                    path,
                                    DBVersion,
                                    version);
                            }
                        } else {
                            THROW_ERROR_EXCEPTION("Error getting version of table %v",
                                path)
                                << ysonVersionOrError;
                        }
                    })));
            }

            WaitFor(Combine(asyncResults))
                .ThrowOnError();
        }
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

                auto idPairs = FindObjectToSweep(typeHandler);
                if (idPairs.empty()) {
                    continue;
                }

                DropRemovedObjects(typeHandler, idPairs);
            }
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Failed to perform removed objects sweep");
        }
    }

    TString BuildSweepQuery(IObjectTypeHandler* typeHandler, TInstant deadline)
    {
        TStringBuilder builder;
        builder.AppendFormat("%v", FormatId(ObjectsTable.Fields.Meta_Id.Name));
        if (typeHandler->GetParentType() != EObjectType::Null) {
            builder.AppendFormat(", %v", FormatId(typeHandler->GetParentIdField()->Name));
        }
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        builder.AppendFormat(" from %v where not is_null(%v) and %v < %v",
            FormatId(ytConnector->GetTablePath(typeHandler->GetTable())),
            FormatId(ObjectsTable.Fields.Meta_RemovalTime.Name),
            FormatId(ObjectsTable.Fields.Meta_RemovalTime.Name),
            deadline.MicroSeconds());
        return builder.Flush();
    }

    std::vector<std::pair<TObjectId, TObjectId>> FindObjectToSweep(IObjectTypeHandler* typeHandler)
    {
        auto deadline = TInstant::Now() - Config_->RemovedObjectsGraceTimeout;
        LOG_INFO("Selecting removed objects to sweep (Type: %v, Deadline: %v)",
            typeHandler->GetType(),
            deadline);

        std::vector<std::pair<TObjectId, TObjectId>> idPairs;

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
            .ValueOrThrow();

        auto* session = transaction->GetSession();
        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                TStringBuilder queryBuilder;
                context->ScheduleSelect(
                    BuildSweepQuery(typeHandler, deadline),
                    [&] (const auto& rowset) {
                        for (auto row : rowset->GetRows()) {
                            auto pair =
                                typeHandler->GetParentType() == EObjectType::Null
                                ? std::make_pair(FromDBValue<TObjectId>(row[0]), TObjectId())
                                : std::make_pair(FromDBValue<TObjectId>(row[0]), FromDBValue<TObjectId>(row[1]));
                            idPairs.push_back(pair);
                        }
                    });
            });
        session->FlushLoads();

        LOG_INFO("Removed objects selected (Count: %v)",
            idPairs.size());

        return idPairs;
    }

    void DropRemovedObjects(IObjectTypeHandler* typeHandler, std::vector<std::pair<TObjectId, TObjectId>>& idPairs)
    {
        LOG_INFO("Starting removal transaction");

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();

        LOG_INFO("Removal transaction started (TransactionId: %v)",
            transaction->GetId());

        auto* session = transaction->GetSession();
        session->ScheduleStore(
            [&] (IStoreContext* context) {
                for (const auto& pair : idPairs) {
                    if (typeHandler->GetParentType() == EObjectType::Null) {
                        context->DeleteRow(
                            typeHandler->GetTable(),
                            ToDBValues(context->GetRowBuffer(), pair.first));
                    } else {
                        context->DeleteRow(
                            typeHandler->GetTable(),
                            ToDBValues(context->GetRowBuffer(), pair.second, pair.first));
                    }
                }
            });

        LOG_INFO("Committing removal transaction");

        WaitFor(transaction->Commit())
            .ThrowOnError();

        LOG_INFO("Removal transaction committed");
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

