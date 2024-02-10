#include "master_connector.h"

#include "helpers.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "operations_cleaner.h"
#include "bootstrap.h"
#include "persistent_scheduler_state.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/server/lib/misc/update_executor.h>

#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/operations_archive_schema.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NRpc;
using namespace NApi;
using namespace NSecurityClient;
using namespace NConcurrency;
using namespace NTransactionServer;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::GetDefaultAddress;


using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("MasterConnector");

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsMasterDisconnectionError(const TError& error)
{
    return error.FindMatching(NObjectClient::EErrorCode::PrerequisiteCheckFailed).has_value();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->SubscribeSynchronized(BIND(&TImpl::OnClusterDirectorySynchronized, MakeWeak(this))
                .Via(Bootstrap_->GetControlInvoker(EControlQueue::MasterConnector)));

        StartConnecting(true);
    }

    EMasterConnectorState GetState() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return State_.load();
    }

    TInstant GetConnectionTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ConnectionTime_.load();
    }

    const NApi::ITransactionPtr& GetLockTransaction() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return LockTransaction_;
    }

    void Disconnect(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoDisconnect(error);
    }

    const IInvokerPtr& GetCancelableControlInvoker(EControlQueue queue) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        return CancelableControlInvokers_[queue];
    }

    void RegisterOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        OperationNodesUpdateExecutor_->AddUpdate(operation->GetId(), TOperationNodeUpdate(operation));
    }

    void UnregisterOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        OperationNodesUpdateExecutor_->RemoveUpdate(operation->GetId());
    }

    int GetYsonNestingLevelLimit() const
    {
        return Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetConfig()
            ->CypressWriteYsonNestingLevelLimit;
    }

    IAttributeDictionaryPtr CreateEphemeralAttributesNestingLimited() const
    {
        return CreateEphemeralAttributes(GetYsonNestingLevelLimit());
    }

    template <typename T>
    TYsonString ConvertToYsonStringNestingLimited(const T& value) const
    {
        return NYson::ConvertToYsonStringNestingLimited(value, GetYsonNestingLevelLimit());
    }

    void DoCreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        auto operationId = operation->GetId();

        try {
            YT_LOG_INFO("Creating operation node (OperationId: %v)",
                operationId);

            {
                auto batchReq = StartObjectBatchRequest();
                bool enableHeavyRuntimeParameters = Config_->EnableHeavyRuntimeParameters;

                auto operationYson = BuildYsonStringFluently()
                    .BeginAttributes()
                        .Do(BIND(&BuildMinimalOperationAttributes, operation))
                        .Item("opaque").Value(true)
                        .Item("runtime_parameters").Value(operation->GetRuntimeParameters(), /*serializeHeavy*/ !enableHeavyRuntimeParameters)
                        .DoIf(enableHeavyRuntimeParameters, [&] (auto fluent) {
                            fluent.Item("heavy_runtime_parameters")
                                .DoMap([&] (auto fluent) {
                                    SerializeHeavyRuntimeParameters(fluent, *operation->GetRuntimeParameters());
                                });
                        })
                        .Item("acl").Value(MakeOperationArtifactAcl(operation->GetRuntimeParameters()->Acl))
                        .Item("has_secure_vault").Value(static_cast<bool>(operation->GetSecureVault()))
                    .EndAttributes()
                    .BeginMap().EndMap();
                ValidateYson(operationYson, GetYsonNestingLevelLimit());

                auto req = TYPathProxy::Set(GetOperationPath(operationId));
                req->set_value(operationYson.ToString());
                req->set_recursive(true);
                req->set_force(true);
                GenerateMutationId(req);
                batchReq->AddRequest(req);

                auto batchRspOrError = WaitFor(batchReq->Invoke());

                GetCumulativeError(batchRspOrError)
                    .ThrowOnError();
            }


            if (operation->GetSecureVault()) {
                MaybeDelay(Config_->TestingOptions->SecureVaultCreationDelay);

                auto batchReq = StartObjectBatchRequest();

                // Create secure vault.
                auto attributes = CreateEphemeralAttributesNestingLimited();
                attributes->Set("inherit_acl", false);
                attributes->Set("value", operation->GetSecureVault());
                attributes->Set("acl", ConvertToYsonString(operation->GetRuntimeParameters()->Acl));

                auto req = TCypressYPathProxy::Create(GetSecureVaultPath(operationId));
                req->set_type(static_cast<int>(EObjectType::Document));
                ToProto(req->mutable_node_attributes(), *attributes);
                GenerateMutationId(req);
                batchReq->AddRequest(req);

                auto batchRspOrError = WaitFor(batchReq->Invoke());

                GetCumulativeError(batchRspOrError)
                    .ThrowOnError();
            }
        } catch (const std::exception& ex) {
            auto error = TError("Error creating operation node %v", operationId)
                << ex;
            if (IsMasterDisconnectionError(error)) {
                error.SetCode(EErrorCode::MasterDisconnected);
            }
            THROW_ERROR error;
        }

        YT_LOG_INFO("Operation node created (OperationId: %v)",
            operationId);
    }

    bool TryReportOperationHeavyAttributesInArchive(const TOperationPtr& operation)
    {
        const auto& initializationAttributes = operation->ControllerAttributes().InitializeAttributes;

        if (!initializationAttributes) {
            return false;
        }

        const auto& fullSpec = initializationAttributes->FullSpec;
        const auto& unrecognizedSpec = initializationAttributes->UnrecognizedSpec;

        auto operationId = operation->GetId();

        const auto& client = Bootstrap_->GetClient();
        auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, TTransactionStartOptions{}))
            .ValueOrThrow();

        YT_LOG_DEBUG("Operation heavy attributes report transaction started (TransactionId: %v, OperationId: %v)",
            transaction->GetId(),
            operationId);

        auto fullSpecString = fullSpec.ToString();
        auto unrecognizedSpecString = unrecognizedSpec.ToString();

        TOrderedByIdTableDescriptor tableDescriptor;
        TUnversionedRowBuilder builder;
        auto operationIdAsGuid = operationId.Underlying();
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[0], tableDescriptor.Index.IdHi));
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[1], tableDescriptor.Index.IdLo));
        builder.AddValue(MakeUnversionedAnyValue(fullSpecString, tableDescriptor.Index.FullSpec));
        builder.AddValue(MakeUnversionedAnyValue(unrecognizedSpecString, tableDescriptor.Index.UnrecognizedSpec));

        auto rowBuffer = New<TRowBuffer>();
        auto row = rowBuffer->CaptureRow(builder.GetRow());
        i64 orderedByIdRowsDataWeight = GetDataWeight(row);

        transaction->WriteRows(
            GetOperationsArchiveOrderedByIdPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(TCompactVector<TUnversionedRow, 1>{1, row}, std::move(rowBuffer)));

        auto error = WaitFor(transaction->Commit()
            .ToUncancelable()
            .WithTimeout(Config_->OperationHeavyAttributesArchivationTimeout));

        if (!error.IsOK()) {
            YT_LOG_WARNING(
                error,
                "Operation heavy attributes report in archive failed (TransactionId: %v, OperationId: %v)",
                transaction->GetId(),
                operationId);
        } else {
            YT_LOG_DEBUG("Operation heavy attributes reported successfully (TransactionId: %v, DataWeight: %v, OperationId: %v)",
                transaction->GetId(),
                orderedByIdRowsDataWeight,
                operationId);
        }
        return error.IsOK();
    }

    TFuture<void> CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        return BIND(&TImpl::DoCreateOperationNode, MakeStrong(this), operation)
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
            .Run();
    }

    TFuture<void> UpdateInitializedOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        auto operationId = operation->GetId();
        YT_LOG_INFO("Updating initialized operation node (OperationId: %v)",
            operationId);

        auto strategy = Bootstrap_->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();

        bool heavyAttributesArchived = false;
        if (Config_->EnableOperationHeavyAttributesArchivation && DoesOperationsArchiveExist()) {
            heavyAttributesArchived = TryReportOperationHeavyAttributesInArchive(operation);
        }

        auto attributes = BuildAttributeDictionaryFluently()
            .Do(BIND(&BuildFullOperationAttributes, operation, /*includeOperationId*/ true, /*includeHeavyAttributes*/ !heavyAttributesArchived))
            .Item("brief_spec").Value(operation->BriefSpecString())
            .Finish();

        auto req = TYPathProxy::MultisetAttributes(GetOperationPath(operationId) + "/@");
        GenerateMutationId(req);
        for (const auto& [attribute, value] : attributes->ListPairs()) {
            auto* subrequest = req->add_subrequests();
            subrequest->set_attribute(attribute);
            ValidateYson(value, GetYsonNestingLevelLimit());
            subrequest->set_value(value.ToString());
        }
        batchReq->AddRequest(req);

        return batchReq->Invoke().Apply(
            BIND(
                &TImpl::OnInitializedOperationNodeUpdated,
                MakeStrong(this),
                operation)
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector)));
    }

    TFuture<void> FlushOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        YT_LOG_INFO("Flushing operation node (OperationId: %v)",
            operation->GetId());

        return OperationNodesUpdateExecutor_->ExecuteUpdate(operation->GetId());
    }

    TFuture<void> FetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        return BIND(&TImpl::DoFetchOperationRevivalDescriptors, MakeStrong(this))
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
            .Run(operations);
    }

    TFuture<TYsonString> GetOperationNodeProgressAttributes(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

        auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), TArchiveOperationRequest::GetProgressAttributeKeys());
        batchReq->AddRequest(req);

        return batchReq->Invoke().Apply(BIND([] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
            auto batchRsp = batchRspOrError
                .ValueOrThrow();
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
            return TYsonString(rsp.Value()->value());
        }));
    }

    TFuture<void> CheckTransactionAlive(TTransactionId transactionId)
    {
        auto cellTag = CellTagFromId(transactionId);
        auto connection = FindRemoteConnection(
            Bootstrap_->GetClient()->GetNativeConnection(),
            cellTag);
        if (!connection) {
            return MakeFuture(TError("Unknown cell tag %v of user transaction", cellTag)
                << TErrorAttribute("transaction_id", transactionId));
        }

        auto proxy = CreateObjectServiceReadProxy(Bootstrap_->GetClient(), EMasterChannelKind::Follower);
        auto batchReq = proxy.ExecuteBatch();
        auto req = TObjectYPathProxy::GetBasicAttributes(FromObjectId(transactionId));
        batchReq->AddRequest(req);

        return batchReq->Invoke().Apply(BIND([transactionId] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
            auto error = TError("Error checking user transaction")
                << TErrorAttribute("transaction_id", transactionId);
            if (!batchRspOrError.IsOK()) {
                THROW_ERROR error
                    << batchRspOrError;
            }

            const auto& batchRsp = batchRspOrError.Value();
            auto rspOrError = batchRsp->GetResponse<TObjectYPathProxy::TRspGetBasicAttributes>(0);
            if (!rspOrError.IsOK()) {
                THROW_ERROR error
                    << rspOrError;
            }
        }));
    }

    void InvokeStoringStrategyState(TPersistentStrategyStatePtr strategyState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        GetCancelableControlInvoker(EControlQueue::MasterConnector)
            ->Invoke(BIND(&TImpl::StorePersistentStrategyState, MakeStrong(this), Passed(std::move(strategyState))));
    }

    void StorePersistentStrategyState(const TPersistentStrategyStatePtr& persistentStrategyState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        if (StoringStrategyState_) {
            YT_LOG_INFO("Skip storing persistent strategy state because the previous attempt hasn't finished yet");

            return;
        }

        StoringStrategyState_ = true;
        auto finally = Finally([&] {
            StoringStrategyState_ = false;
        });

        YT_LOG_INFO("Storing persistent strategy state");

        auto batchReq = StartObjectBatchRequest();

        auto req = NCypressClient::TCypressYPathProxy::Create(StrategyStatePath);
        req->set_type(static_cast<int>(EObjectType::Document));
        req->set_force(true);

        auto* attribute = req->mutable_node_attributes()->add_attributes();
        attribute->set_key("value");
        attribute->set_value(ConvertToYsonStringNestingLimited(persistentStrategyState).ToString());

        GenerateMutationId(req);
        batchReq->AddRequest(req);

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_ERROR(rspOrError, "Error storing persistent strategy state");
        } else {
            YT_LOG_INFO("Persistent strategy state successfully stored");
        }
    }

    void DoUpdateLastMeteringLogTime(TInstant time)
    {
        auto batchReq = StartObjectBatchRequest();

        auto req = TYPathProxy::Set(LastMeteringLogTimePath);
        req->set_value(ConvertToYsonStringNestingLimited(time).ToString());
        GenerateMutationId(req);
        batchReq->AddRequest(req);

        GetCumulativeError(WaitFor(batchReq->Invoke()))
            .ThrowOnError();

        YT_LOG_INFO("Last metering log time written to Cypress (LastMeteringLogTime: %v)", time);
    }

    TFuture<void> UpdateLastMeteringLogTime(TInstant time)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoUpdateLastMeteringLogTime, MakeStrong(this))
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
            .Run(time);
    }

    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto savedAlert = alert;
        savedAlert.MutableAttributes()->Set("alert_type", alertType);
        Alerts_[alertType] = std::move(savedAlert);
    }

    void AddCommonWatcher(
        TWatcherRequester requester,
        TWatcherHandler handler,
        std::optional<ESchedulerAlertType> alertType)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CommonWatcherRecords_.push_back(TWatcherRecord{requester, handler, alertType});
    }

    void SetCustomWatcher(
        EWatcherType type,
        TWatcherRequester requester,
        TWatcherHandler handler,
        TDuration period,
        std::optional<ESchedulerAlertType> alertType,
        std::optional<TWatcherLockOptions> lockOptions)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CustomWatcherRecords_[type] = TCustomWatcherRecord{
            TWatcherRecord{
                .Requester = std::move(requester),
                .Handler = std::move(handler),
                .AlertType = alertType,
            },
            /*WatcherType*/ type,
            /*Period*/ period,
            /*LockOptions*/ lockOptions
        };
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (State_ == EMasterConnectorState::Connected &&
            Config_->LockTransactionTimeout != config->LockTransactionTimeout)
        {
            YT_UNUSED_FUTURE(BIND(&TImpl::UpdateLockTransactionTimeout, MakeStrong(this), config->LockTransactionTimeout)
                .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
                .Run());
        }

        Config_ = config;

        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->SetPeriod(Config_->OperationsUpdatePeriod);
        }
        if (CommonWatchersExecutor_) {
            CommonWatchersExecutor_->SetPeriod(Config_->WatchersUpdatePeriod);
        }
        if (AlertsExecutor_) {
            AlertsExecutor_->SetPeriod(Config_->AlertsUpdatePeriod);
        }
        if (CustomWatcherExecutors_[EWatcherType::NodeAttributes]) {
            CustomWatcherExecutors_[EWatcherType::NodeAttributes]->SetPeriod(Config_->NodesAttributesUpdatePeriod);
            CustomWatcherRecords_[EWatcherType::NodeAttributes].Period = Config_->NodesAttributesUpdatePeriod;
            CustomWatcherExecutors_[EWatcherType::PoolTrees]->SetPeriod(Config_->WatchersUpdatePeriod);
            CustomWatcherRecords_[EWatcherType::PoolTrees].Period = Config_->WatchersUpdatePeriod;
        }

        ScheduleTestingDisconnect();
    }

    DEFINE_SIGNAL(void(), MasterConnecting);
    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterHandshake);
    DEFINE_SIGNAL(void(), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

private:
    TSchedulerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    TCancelableContextPtr CancelableContext_;
    TEnumIndexedArray<EControlQueue, IInvokerPtr> CancelableControlInvokers_;

    std::atomic<EMasterConnectorState> State_ = {EMasterConnectorState::Disconnected};
    std::atomic<TInstant> ConnectionTime_ = {};

    ITransactionPtr LockTransaction_;

    TPeriodicExecutorPtr CommonWatchersExecutor_;
    TPeriodicExecutorPtr AlertsExecutor_;

    struct TWatcherRecord
    {
        TWatcherRequester Requester;
        TWatcherHandler Handler;
        std::optional<ESchedulerAlertType> AlertType;
    };

    struct TCustomWatcherRecord
        : public TWatcherRecord
    {
        EWatcherType WatcherType;
        TDuration Period;
        std::optional<TWatcherLockOptions> LockOptions;
    };

    std::vector<TWatcherRecord> CommonWatcherRecords_;

    TEnumIndexedArray<EWatcherType, TCustomWatcherRecord> CustomWatcherRecords_;
    TEnumIndexedArray<EWatcherType, TPeriodicExecutorPtr> CustomWatcherExecutors_;

    TEnumIndexedArray<ESchedulerAlertType, TError> Alerts_;

    std::optional<bool> ArchiveExists_;

    struct TOperationNodeUpdate
    {
        explicit TOperationNodeUpdate(TOperationPtr operation)
            : Operation(std::move(operation))
        { }

        TOperationPtr Operation;
    };

    TIntrusivePtr<TUpdateExecutor<TOperationId, TOperationNodeUpdate>> OperationNodesUpdateExecutor_;

    bool StoringStrategyState_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void ScheduleTestingDisconnect()
    {
        if (Config_->TestingOptions->EnableRandomMasterDisconnection) {
            TDelayedExecutor::Submit(
                BIND(&TImpl::RandomDisconnect, MakeStrong(this))
                    .Via(Bootstrap_->GetControlInvoker(EControlQueue::MasterConnector)),
                RandomDuration(Config_->TestingOptions->RandomMasterDisconnectionMaxBackoff));
        }
    }

    void RandomDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Config_->TestingOptions->EnableRandomMasterDisconnection) {
            DoDisconnect(TError("Disconnecting scheduler due to enabled random disconnection"));
        }
    }

    void StartConnecting(bool immediate)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::DoStartConnecting, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker(EControlQueue::MasterConnector)),
            immediate ? TDuration::Zero() : Config_->ConnectRetryBackoffTime);
    }

    void DoStartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (State_ != EMasterConnectorState::Disconnected) {
            return;
        }
        State_ = EMasterConnectorState::Connecting;

        YT_LOG_INFO("Connecting to master");

        YT_VERIFY(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();

        for (auto queue : TEnumTraits<EControlQueue>::GetDomainValues()) {
            YT_VERIFY(!CancelableControlInvokers_[queue]);
            CancelableControlInvokers_[queue] = CancelableContext_->CreateInvoker(
                Bootstrap_->GetControlInvoker(queue));
        }

        OperationNodesUpdateExecutor_ = New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            GetCancelableControlInvoker(EControlQueue::OperationsPeriodicActivity),
            BIND(&TImpl::UpdateOperationNode, Unretained(this)),
            BIND([] (const TOperationNodeUpdate*) { return false; }),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Config_->OperationsUpdatePeriod,
            Logger);

        CommonWatchersExecutor_ = New<TPeriodicExecutor>(
            GetCancelableControlInvoker(EControlQueue::CommonPeriodicActivity),
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config_->WatchersUpdatePeriod);

        AlertsExecutor_ = New<TPeriodicExecutor>(
            GetCancelableControlInvoker(EControlQueue::CommonPeriodicActivity),
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config_->AlertsUpdatePeriod);

        for (const auto& record : CustomWatcherRecords_) {
            auto executor = New<TPeriodicExecutor>(
                GetCancelableControlInvoker(EControlQueue::CommonPeriodicActivity),
                BIND(&TImpl::ExecuteCustomWatcherUpdate, MakeWeak(this), record, /*strictMode*/ false),
                record.Period);
            CustomWatcherExecutors_[record.WatcherType] = executor;
        }

        auto pipeline = New<TRegistrationPipeline>(this);
        BIND(&TRegistrationPipeline::Run, pipeline)
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
            .Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this))
                .Via(GetCancelableControlInvoker(EControlQueue::MasterConnector)));
    }

    void OnConnected(const TError& error) noexcept
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ == EMasterConnectorState::Connecting);

        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Error connecting to master");
            DoCleanup();
            StartConnecting(false);
            return;
        }

        TForbidContextSwitchGuard contextSwitchGuard;

        State_.store(EMasterConnectorState::Connected);
        ConnectionTime_.store(TInstant::Now());

        YT_LOG_INFO("Master connected");

        LockTransaction_->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(GetCancelableControlInvoker(EControlQueue::MasterConnector)));

        StartPeriodicActivities();

        MasterConnected_.Fire();

        ScheduleTestingDisconnect();
    }

    void OnLockTransactionAborted(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Disconnect(TError("Lock transaction aborted")
            << error);
    }

    bool DoesOperationsArchiveExist()
    {
        if (!ArchiveExists_) {
            ArchiveExists_ = Bootstrap_->GetClient()->DoesOperationsArchiveExist();
        }
        return *ArchiveExists_;
    }


    class TRegistrationPipeline
        : public TRefCounted
    {
    public:
        explicit TRegistrationPipeline(TIntrusivePtr<TImpl> owner)
            : Owner_(std::move(owner))
            , ServiceAddresses_(Owner_->Bootstrap_->GetLocalAddresses())
        { }

        void Run()
        {
            FireConnecting();
            EnsureNoSafeMode();
            RegisterInstance();
            StartLockTransaction();
            TakeLock();
            AssumeControl();
            StrictUpdateWatchers();
            InitPersistentStrategyState();
            SyncClusterDirectory();
            SyncMediumDirectory();
            ListOperations();
            RequestOperationAttributes();
            RequestLastMeteringLogTime();
            FireHandshake();
        }

    private:
        const TIntrusivePtr<TImpl> Owner_;
        const TAddressMap ServiceAddresses_;

        std::vector<TOperationId> OperationIds_;

        TMasterHandshakeResult Result_;

        void FireConnecting()
        {
            Owner_->MasterConnecting_.Fire();
        }

        void EnsureNoSafeMode()
        {
            auto proxy = CreateObjectServiceReadProxy(
                Owner_->Bootstrap_->GetClient(),
                EMasterChannelKind::Follower);
            auto req = TCypressYPathProxy::Get("//sys/@config/enable_safe_mode");
            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting \"enable_safe_mode\" from master");

            bool safeMode = ConvertTo<bool>(TYsonString(rspOrError.Value()->value()));
            if (safeMode) {
                THROW_ERROR_EXCEPTION("Cluster is in safe mode");
            }
        }

        // - Register scheduler instance.
        void RegisterInstance()
        {
            auto proxy = CreateObjectServiceWriteProxy(Owner_->Bootstrap_->GetClient());
            auto batchReq = proxy.ExecuteBatch();
            auto path = "//sys/scheduler/instances/" + ToYPathLiteral(GetDefaultAddress(ServiceAddresses_));
            {
                auto req = TCypressYPathProxy::Create(path);
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::MapNode));
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Set(path + "/@annotations");
                req->set_value(Owner_->ConvertToYsonStringNestingLimited(Owner_->Bootstrap_->GetConfig()->CypressAnnotations).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Create(path + "/orchid");
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::Orchid));
                auto attributes = Owner_->CreateEphemeralAttributesNestingLimited();
                attributes->Set("remote_addresses", ServiceAddresses_);
                ToProto(req->mutable_node_attributes(), *attributes);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }

        // - Start lock transaction.
        void StartLockTransaction()
        {
            TTransactionStartOptions options;
            options.AutoAbort = true;
            options.Timeout = Owner_->Config_->LockTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Scheduler lock at %v", GetDefaultAddress(ServiceAddresses_)));
            options.Attributes = std::move(attributes);

            auto client = Owner_->Bootstrap_->GetClient();
            auto transactionOrError = WaitFor(Owner_->Bootstrap_->GetClient()->StartTransaction(
                ETransactionType::Master,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting lock transaction");

            Owner_->LockTransaction_ = transactionOrError.Value();

            YT_LOG_INFO("Lock transaction is %v", Owner_->LockTransaction_->GetId());
        }

        // - Take lock.
        void TakeLock()
        {
            auto result = WaitFor(Owner_->LockTransaction_->LockNode("//sys/scheduler/lock", ELockMode::Exclusive));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error taking scheduler lock");
        }

        // - Publish scheduler address.
        // - Update orchid address.
        void AssumeControl()
        {
            auto batchReq = Owner_->StartObjectBatchRequest();
            auto addresses = Owner_->Bootstrap_->GetLocalAddresses();
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@addresses");
                req->set_value(Owner_->ConvertToYsonStringNestingLimited(addresses).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid&/@remote_addresses");
                req->set_value(Owner_->ConvertToYsonStringNestingLimited(addresses).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@connection_time");
                req->set_value(Owner_->ConvertToYsonStringNestingLimited(TInstant::Now()).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }

        void SyncClusterDirectory()
        {
            YT_LOG_INFO("Sync cluster directory started");
            WaitFor(Owner_
                ->Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetClusterDirectorySynchronizer()
                ->Sync(/*force*/ true))
                .ThrowOnError();
            YT_LOG_INFO("Sync cluster directory finished");
        }

        void SyncMediumDirectory()
        {
            YT_LOG_INFO("Sync medium directory started");
            WaitFor(Owner_
                ->Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetMediumDirectorySynchronizer()
                ->NextSync(/*force*/ true))
                .ThrowOnError();
            YT_LOG_INFO("Sync medium directory finished");
        }

        // - Request operations and their states.
        void ListOperations()
        {
            YT_LOG_INFO("Started listing existing operations");

            auto createBatchRequest = BIND(
                &TImpl::StartObjectBatchRequest,
                Owner_,
                EMasterChannelKind::Follower,
                PrimaryMasterCellTagSentinel,
                /*subbatchSize*/ 100);

            auto listOperationsResult = NScheduler::ListOperations(createBatchRequest);
            OperationIds_.reserve(listOperationsResult.OperationsToRevive.size());

            for (const auto& [operationId, state] : listOperationsResult.OperationsToRevive) {
                YT_LOG_DEBUG("Found operation in Cypress (OperationId: %v, State: %v)",
                    operationId,
                    state);
                OperationIds_.push_back(operationId);
            }

            auto operationsCleaner = Owner_->Bootstrap_->GetScheduler()->GetOperationsCleaner();
            operationsCleaner->SubmitForArchivation(std::move(listOperationsResult.OperationsToArchive));

            YT_LOG_INFO("Finished listing existing operations");
        }

        struct TOperationDataToParse final
        {
            TYsonString AttributesYson;
            TYsonString SecureVaultYson;
            TOperationId OperationId;
        };

        struct TProcessedOperationsBatch final
        {
            std::vector<TOperationPtr> Operations;
            std::vector<TOperationId> IncompleteOperationIds;
        };

        TProcessedOperationsBatch ProcessOperationsBatch(
            const std::vector<TOperationDataToParse>& rspValuesChunk,
            const int parseOperationAttributesBatchSize,
            const bool skipOperationsWithMalformedSpecDuringRevival,
            const TSerializableAccessControlList& operationBaseAcl,
            const IInvokerPtr& cancelableOperationInvoker)
        {
            TProcessedOperationsBatch result;
            result.Operations.reserve(parseOperationAttributesBatchSize);

            for (const auto& rspValues : rspValuesChunk) {
                auto attributesNode = ConvertToAttributes(rspValues.AttributesYson);

                IMapNodePtr secureVault;

                if (rspValues.SecureVaultYson) {
                    auto secureVaultNode = ConvertToNode(rspValues.SecureVaultYson);
                    // It is a pretty strange situation when the node type different
                    // from map, but still we should consider it.
                    if (secureVaultNode->GetType() == ENodeType::Map) {
                        secureVault = secureVaultNode->AsMap();
                    } else {
                        // TODO(max42): (YT-5651) Do not just ignore such a situation!
                        YT_LOG_WARNING("Invalid secure vault node type (OperationId: %v, ActualType: %v, ExpectedType: %v)",
                            rspValues.OperationId,
                            secureVaultNode->GetType(),
                            ENodeType::Map);
                    }
                } else if (attributesNode->Get<bool>("has_secure_vault", false)) {
                    YT_LOG_INFO("Operation secure vault is missing; operation skipped (OperationId: %v)", rspValues.OperationId);
                    result.IncompleteOperationIds.push_back(rspValues.OperationId);
                    continue;
                }

                try {
                    if (attributesNode->Get<bool>("banned", false)) {
                        YT_LOG_INFO("Operation manually banned (OperationId: %v)", rspValues.OperationId);
                        continue;
                    }
                    auto operation = TryCreateOperationFromAttributes(
                        rspValues.OperationId,
                        *attributesNode,
                        secureVault,
                        operationBaseAcl,
                        cancelableOperationInvoker);
                    result.Operations.push_back(operation);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex, "Error creating operation from Cypress node (OperationId: %v)",
                        rspValues.OperationId);
                    if (!skipOperationsWithMalformedSpecDuringRevival) {
                        throw;
                    }
                }
            }

            return result;
        }

        // - Request attributes for unfinished operations.
        // - Recreate operation instance from fetched data.
        void RequestOperationAttributes()
        {
            // Keep stuff below in sync with #TryCreateOperationFromAttributes.
            static const std::vector<TString> attributeKeys = {
                "operation_type",
                "mutation_id",
                "user_transaction_id",
                "spec",
                "experiment_assignments",
                "authenticated_user",
                "start_time",
                "state",
                "events",
                "slot_index_per_pool_tree",
                "runtime_parameters",
                "heavy_runtime_parameters",
                "output_completion_transaction_id",
                "suspended",
                "erased_trees",
                "banned",
                // COMPAT(eshcherbin)
                "initial_aggregated_min_needed_resources",
                "registration_index",
                "alerts",
                "provided_spec",
                "has_secure_vault",
            };
            const int operationsCount = static_cast<int>(OperationIds_.size());

            std::vector<TOperationId> operationIdsToRemove;

            YT_LOG_INFO("Fetching attributes and secure vaults for unfinished operations (UnfinishedOperationCount: %v)",
                operationsCount);

            auto batchReq = Owner_->StartObjectBatchRequest(
                EMasterChannelKind::Follower,
                PrimaryMasterCellTagSentinel,
                Owner_->Config_->FetchOperationAttributesSubbatchSize);
            THashMap<TOperationId, size_t> startResponseIndex;

            enum class ERequestPart {
                Attributes = 0,
                SecureVault = 1,
                NumOfParts = 2
            };

            startResponseIndex.reserve(OperationIds_.size());
            {
                for (int index = 0; index < operationsCount; ++index) {
                    const auto& operationId = OperationIds_[index];
                    startResponseIndex[operationId] = index;

                    // Keep stuff below in sync with #TryCreateOperationFromAttributes.

                    auto operationAttributesPath = GetOperationPath(operationId) + "/@";
                    auto secureVaultPath = GetSecureVaultPath(operationId);

                    // Retrieve operation attributes.
                    {
                        auto req = TYPathProxy::Get(operationAttributesPath);
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req);
                    }

                    // Retrieve secure vault.
                    {
                        auto req = TYPathProxy::Get(secureVaultPath);
                        batchReq->AddRequest(req);
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            YT_LOG_INFO("Attributes for unfinished operations fetched");

            {
                const auto chunkSize = Owner_->Config_->ParseOperationAttributesBatchSize;

                std::vector<TFuture<TProcessedOperationsBatch>> futures;
                futures.reserve(RoundUp(operationsCount, chunkSize));

                for (auto startIndex = 0; startIndex < operationsCount; startIndex += chunkSize) {
                    std::vector<TOperationDataToParse> operationsDataToProcessBatch;

                    operationsDataToProcessBatch.reserve(chunkSize);
                    for (auto index = startIndex; index < std::min(startIndex + chunkSize, operationsCount); ++index) {
                        const auto& operationId = OperationIds_[index];

                        const auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                                startResponseIndex[operationId] * static_cast<int>(ERequestPart::NumOfParts) +
                                static_cast<int>(ERequestPart::Attributes)
                            )
                            .ValueOrThrow();

                        const auto secureVaultRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                                startResponseIndex[operationId] * static_cast<int>(ERequestPart::NumOfParts) +
                                static_cast<int>(ERequestPart::SecureVault));

                        if (!secureVaultRspOrError.IsOK() &&
                            secureVaultRspOrError.GetCode() != NYTree::EErrorCode::ResolveError) {
                            THROW_ERROR_EXCEPTION("Error while attempting to fetch the secure vault of operation (OperationId: %v)",
                                operationId)
                                << secureVaultRspOrError;
                        }

                        auto atttibutesNodeStr = TYsonString(attributesRsp->value());
                        TYsonString secureVaultYson;
                        if (secureVaultRspOrError.IsOK()) {
                            secureVaultYson = TYsonString(secureVaultRspOrError.Value()->value());
                        }

                        operationsDataToProcessBatch.push_back({std::move(atttibutesNodeStr), std::move(secureVaultYson), operationId});
                    }

                    futures.push_back(BIND(
                            &TRegistrationPipeline::ProcessOperationsBatch,
                            MakeStrong(this),
                            std::move(operationsDataToProcessBatch),
                            chunkSize,
                            Owner_->Config_->SkipOperationsWithMalformedSpecDuringRevival,
                            Owner_->Bootstrap_->GetScheduler()->GetOperationBaseAcl(),
                            Owner_->GetCancelableControlInvoker(EControlQueue::Operation)
                        )
                        .AsyncVia(Owner_->Bootstrap_->GetScheduler()->GetBackgroundInvoker())
                        .Run()
                    );
                }
                YT_LOG_INFO("Operation attributes batches for parsing formed");

                Result_.Operations.reserve(OperationIds_.size());
                auto result = WaitFor(AllSucceeded(futures)).ValueOrThrow();

                for (auto& processedBatch : result) {
                    for (auto& operation : processedBatch.Operations) {
                        Result_.Operations.push_back(std::move(operation));
                    }
                    for (auto operationId : processedBatch.IncompleteOperationIds) {
                        operationIdsToRemove.push_back(operationId);
                    }
                }
            }

            std::sort(
                Result_.Operations.begin(),
                Result_.Operations.end(),
                [] (const TOperationPtr& lhs, const TOperationPtr& rhs) {
                    // Remind that:
                    // 1. Starting operations have no slot index and should be processed after all other operations
                    //    to avoid slot index changes.
                    // 2. Pending operations should be processes after running operations
                    //    to save current list of pending operations.
                    if (lhs->GetState() != rhs->GetState()) {
                        return static_cast<int>(lhs->GetState()) > static_cast<int>(rhs->GetState());
                    }
                    // Registration index is used for testing purposes.
                    if (lhs->RegistrationIndex() != rhs->RegistrationIndex()) {
                        return lhs->RegistrationIndex() < rhs->RegistrationIndex();
                    }
                    // We should sort operation by start time to respect pending operation queues.
                    return lhs->GetStartTime() < rhs->GetStartTime();
                });

            auto operationsCleaner = Owner_->Bootstrap_->GetScheduler()->GetOperationsCleaner();
            operationsCleaner->SubmitForRemoval(std::move(operationIdsToRemove));

            YT_LOG_INFO("Operation objects created from attributes");
        }

        TOperationPtr TryCreateOperationFromAttributes(
            TOperationId operationId,
            const IAttributeDictionary& attributes,
            const IMapNodePtr& secureVault,
            const TSerializableAccessControlList& operationBaseAcl,
            const IInvokerPtr& cancelableOperationInvoker)
        {
            auto specString = attributes.GetYson("spec");
            auto providedSpecString = attributes.FindYson("provided_spec");

            // COMPAT(gepardo): can be removed when all the running operation will have provided_spec field.
            if (!providedSpecString) {
                providedSpecString = specString;
            }

            auto specNode = ConvertSpecStringToNode(specString);
            auto operationType = attributes.Get<EOperationType>("operation_type");
            TPreprocessedSpec preprocessedSpec;
            ParseSpec(std::move(specNode), /*specTemplate*/ nullptr, operationType, operationId, &preprocessedSpec);
            preprocessedSpec.ExperimentAssignments =
                attributes.Get<std::vector<TExperimentAssignmentPtr>>("experiment_assignments", {});
            const auto& spec = preprocessedSpec.Spec;

            // NB: Keep stuff below in sync with #RequestOperationAttributes.
            auto user = attributes.Get<TString>("authenticated_user");

            YT_VERIFY(attributes.Contains("runtime_parameters"));

            TOperationRuntimeParametersPtr runtimeParameters;
            if (auto heavyRuntimeParameters = attributes.Find<IMapNodePtr>("heavy_runtime_parameters")) {
                auto runtimeParametersNode = attributes.Get<IMapNodePtr>("runtime_parameters");
                runtimeParameters = ConvertTo<TOperationRuntimeParametersPtr>(
                    PatchNode(runtimeParametersNode, heavyRuntimeParameters));
            } else {
                runtimeParameters = attributes.Get<TOperationRuntimeParametersPtr>("runtime_parameters");
            }

            auto baseAcl = operationBaseAcl;
            if (spec->AddAuthenticatedUserToAcl) {
                baseAcl.Entries.emplace_back(
                    ESecurityAction::Allow,
                    std::vector<TString>{user},
                    EPermissionSet(EPermission::Read | EPermission::Manage));
            }

            auto operation = NewWithOffloadedDtor<TOperation>(
                Owner_->Bootstrap_->GetScheduler()->GetBackgroundInvoker(),
                operationId,
                operationType,
                attributes.Get<TMutationId>("mutation_id"),
                attributes.Get<TTransactionId>("user_transaction_id"),
                spec,
                std::move(preprocessedSpec.CustomSpecPerTree),
                std::move(preprocessedSpec.SpecString),
                std::move(preprocessedSpec.TrimmedAnnotations),
                std::move(preprocessedSpec.VanillaTaskNames),
                secureVault,
                runtimeParameters,
                std::move(baseAcl),
                user,
                attributes.Get<TInstant>("start_time"),
                cancelableOperationInvoker,
                spec->Alias,
                std::move(preprocessedSpec.ExperimentAssignments),
                providedSpecString,
                attributes.Get<EOperationState>("state"),
                attributes.Get<std::vector<TOperationEvent>>("events", {}),
                attributes.Get<bool>("suspended", false),
                attributes.Find<TJobResources>("initial_aggregated_min_needed_resources"),
                attributes.Get<int>("registration_index", 0),
                attributes.Get<THashMap<EOperationAlertType, TOperationAlert>>("alerts", {}));


            operation->SetShouldFlushAcl(true);

            auto slotIndexMap = attributes.Find<THashMap<TString, int>>("slot_index_per_pool_tree");
            if (slotIndexMap) {
                for (const auto& [treeId, slotIndex] : *slotIndexMap) {
                    operation->SetSlotIndex(treeId, slotIndex);
                }
            }

            // NB: Keep stuff above in sync with #RequestOperationAttributes.

            return operation;
        }

        void InitPersistentStrategyState()
        {
            try {
                DoInitPersistentStrategyState();
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                Owner_->SetSchedulerAlert(ESchedulerAlertType::SchedulerCannotConnect, error);
                Owner_->UpdateAlerts();
                THROW_ERROR(error);
            }
        }

        void DoInitPersistentStrategyState()
        {
            YT_LOG_INFO("Requesting persistent strategy state");

            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            batchReq->AddRequest(TYPathProxy::Get(StrategyStatePath), "get_strategy_state");

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_strategy_state");
            if (!rspOrError.IsOK() && !rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR(rspOrError.Wrap(EErrorCode::WatcherHandlerFailed, "Error fetching strategy state"));
            }

            TPersistentStrategyStatePtr strategyState;
            {
                strategyState = New<TPersistentStrategyState>();
                if (rspOrError.IsOK()) {
                    auto value = rspOrError.ValueOrThrow()->value();
                    try {
                        strategyState = ConvertTo<TPersistentStrategyStatePtr>(TYsonString(value));
                        YT_LOG_INFO("Successfully fetched strategy state");
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(
                            ex,
                            "Failed to deserialize strategy state; will drop it (Value: %v)",
                            ConvertToYsonString(value, EYsonFormat::Text));
                    }
                }
            }

            Owner_->Bootstrap_->GetScheduler()->GetStrategy()->InitPersistentState(strategyState);
        }

        void StrictUpdateWatchers()
        {
            YT_LOG_INFO("Request common watcher updates");
            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (const auto& watcher : Owner_->CommonWatcherRecords_) {
                watcher.Requester(batchReq);
            }

            auto watcherResponses = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            YT_LOG_INFO("Handling common watcher update results");

            for (const auto& watcher : Owner_->CommonWatcherRecords_) {
                Owner_->RunWatcherHandler(watcher, watcherResponses, /*strictMode*/ true);
            }

            YT_LOG_INFO("Common watchers update results handled");

            for (const auto& watcher : Owner_->CustomWatcherRecords_) {
                YT_LOG_INFO("Updating custom watcher (WatcherType: %v)", watcher.WatcherType);
                Owner_->ExecuteCustomWatcherUpdate(watcher, /*strictMode*/ true);
                YT_LOG_INFO("Custom watcher updated (WatcherType: %v)", watcher.WatcherType);
            }

            Owner_->SetSchedulerAlert(ESchedulerAlertType::SchedulerCannotConnect, TError());
        }

        void FireHandshake()
        {
            try {
                Owner_->MasterHandshake_.Fire(Result_);
            } catch (const std::exception&) {
                YT_LOG_WARNING("Master handshake failed, disconnecting scheduler");
                Owner_->MasterDisconnected_.Fire();
                throw;
            }

        }

        void RequestLastMeteringLogTime()
        {
            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            batchReq->AddRequest(TYPathProxy::Get(LastMeteringLogTimePath), "get_last_metering_log_time");

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_last_metering_log_time");
            if (!rspOrError.IsOK()) {
                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    YT_LOG_INFO(rspOrError, "Last metering log time is missing");
                    Result_.LastMeteringLogTime = TInstant::Now();
                } else {
                    rspOrError.ThrowOnError();
                }
            } else {
                Result_.LastMeteringLogTime = ConvertTo<TInstant>(TYsonString(rspOrError.ValueOrThrow()->value()));
                YT_LOG_INFO("Last metering log time read from Cypress (LastMeteringLogTime: %v)",
                    Result_.LastMeteringLogTime);
            }
        }
    };

    void GetTransactionsAndRevivalDescriptor(const TOperationPtr& operation, IAttributeDictionaryPtr attributes)
    {
        auto operationId = operation->GetId();
        auto attachTransaction = [&] (TTransactionId transactionId, bool ping, const TString& name = TString()) -> ITransactionPtr {
            if (!transactionId) {
                if (name) {
                    YT_LOG_DEBUG("Missing %v transaction (OperationId: %v)",
                        name,
                        operationId,
                        transactionId);
                }
                return nullptr;
            }
            try {
                auto client = Bootstrap_->GetRemoteClient(CellTagFromId(transactionId));

                TTransactionAttachOptions options;
                options.PingPeriod = Config_->OperationTransactionPingPeriod;
                options.Ping = ping;
                options.PingAncestors = false;
                auto transaction = client->AttachTransaction(transactionId, options);
                WaitFor(transaction->Ping())
                    .ThrowOnError();
                return transaction;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error attaching operation transaction (OperationId: %v, TransactionId: %v)",
                    operationId,
                    transactionId);
                return nullptr;
            }
        };

        TOperationTransactions transactions;
        TOperationRevivalDescriptor revivalDescriptor;
        transactions.AsyncTransaction = attachTransaction(
            attributes->Get<TTransactionId>("async_scheduler_transaction_id", NullTransactionId),
            true,
            "async");
        transactions.InputTransaction = attachTransaction(
            attributes->Get<TTransactionId>("input_transaction_id", NullTransactionId),
            true,
            "input");
        transactions.OutputTransaction = attachTransaction(
            attributes->Get<TTransactionId>("output_transaction_id", NullTransactionId),
            true,
            "output");
        transactions.OutputCompletionTransaction = attachTransaction(
            attributes->Get<TTransactionId>("output_completion_transaction_id", NullTransactionId),
            true,
            "output completion");
        transactions.DebugTransaction = attachTransaction(
            attributes->Get<TTransactionId>("debug_transaction_id", NullTransactionId),
            true,
            "debug");
        transactions.DebugCompletionTransaction = attachTransaction(
            attributes->Get<TTransactionId>("debug_completion_transaction_id", NullTransactionId),
            true,
            "debug completion");

        auto nestedInputTransactionIds = attributes->Get<std::vector<TTransactionId>>("nested_input_transaction_ids", {});
        THashMap<TTransactionId, ITransactionPtr> transactionIdToTransaction;
        for (auto transactionId : nestedInputTransactionIds) {
            auto it = transactionIdToTransaction.find(transactionId);
            if (it == transactionIdToTransaction.end()) {
                auto transaction = attachTransaction(
                    transactionId,
                    true,
                    "nested input transaction"
                );
                YT_VERIFY(transactionIdToTransaction.emplace(transactionId, transaction).second);
                transactions.NestedInputTransactions.push_back(transaction);
            } else {
                transactions.NestedInputTransactions.push_back(it->second);
            }
        }

        const auto& userTransactionId = operation->GetUserTransactionId();
        auto userTransaction = attachTransaction(userTransactionId, false);

        revivalDescriptor.UserTransactionAborted = !userTransaction && userTransactionId;

        for (const auto& event : operation->Events()) {
            if (event.State == EOperationState::Aborting) {
                revivalDescriptor.OperationAborting = true;
                break;
            }
        }

        operation->RevivalDescriptor() = std::move(revivalDescriptor);
        operation->Transactions() = std::move(transactions);
    }

    void DoFetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Fetching operation revival descriptors (OperationCount: %v)",
            operations.size());

        {
            static const std::vector<TString> attributeKeys = {
                "async_scheduler_transaction_id",
                "input_transaction_id",
                "output_transaction_id",
                "debug_transaction_id",
                "output_completion_transaction_id",
                "debug_completion_transaction_id",
                "nested_input_transaction_ids",
            };

            auto batchReq = StartObjectBatchRequest(
                EMasterChannelKind::Follower,
                PrimaryMasterCellTagSentinel,
                Config_->FetchOperationAttributesSubbatchSize);

            for (const auto& operation : operations) {
                auto operationId = operation->GetId();
                auto operationAttributesPath = GetOperationPath(operationId) + "/@";
                auto secureVaultPath = GetSecureVaultPath(operationId);

                // Retrieve operation attributes.
                {
                    auto req = TYPathProxy::Get(operationAttributesPath);
                    ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                    batchReq->AddRequest(req, "get_op_attr_" + ToString(operationId));
                }
            }

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            YT_LOG_INFO("Fetched operation transaction ids, starting to ping them (OperationCount: %v)",
                operations.size());

            std::vector<TFuture<void>> futures;
            for (const auto& operation : operations) {
                auto operationId = operation->GetId();

                auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                    "get_op_attr_" + ToString(operationId))
                    .ValueOrThrow();

                IAttributeDictionaryPtr attributes;
                try {
                    attributes = ConvertToAttributes(TYsonString(attributesRsp->value()));
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing attributes of operation")
                        << TErrorAttribute("operation_id", operationId)
                        << ex;
                }
                futures.push_back(
                    BIND(&TImpl::GetTransactionsAndRevivalDescriptor, MakeStrong(this))
                        .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
                        .Run(operation, attributes)
                );
            }
            WaitFor(AllSucceeded(futures))
                .ThrowOnError();
        }

        std::vector<TOperationPtr> operationsToFetchCommittedFlag;
        for (const auto& operation : operations) {
            auto eventIt = operation->Events().rbegin();
            while (eventIt != operation->Events().rend() && eventIt->State == EOperationState::Orphaned) {
                ++eventIt;
            }
            if (eventIt != operation->Events().rend() && eventIt->State == EOperationState::Completing) {
                operationsToFetchCommittedFlag.push_back(operation);
            }
        }

        YT_LOG_INFO("Fetching committed flags (OperationCount: %v)",
            operationsToFetchCommittedFlag.size());

        {
            auto getBatchKey = [] (const TOperationPtr& operation) {
                return "get_op_committed_attr_" + ToString(operation->GetId());
            };

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

            for (const auto& operation : operationsToFetchCommittedFlag) {
                const auto& transactions = *operation->Transactions();
                std::vector<TTransactionId> possibleTransactions;
                if (transactions.OutputTransaction) {
                    possibleTransactions.push_back(transactions.OutputTransaction->GetId());
                }
                if (operation->GetUserTransactionId()) {
                    possibleTransactions.push_back(operation->GetUserTransactionId());
                }
                possibleTransactions.push_back(NullTransactionId);

                for (auto transactionId : possibleTransactions)
                {
                    auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
                    ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{CommittedAttribute});
                    SetTransactionId(req, transactionId);
                    batchReq->AddRequest(req, getBatchKey(operation));
                }
            }

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            for (const auto& operation : operationsToFetchCommittedFlag) {
                auto& revivalDescriptor = *operation->RevivalDescriptor();
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(operation));

                for (size_t rspIndex = 0; rspIndex < rsps.size(); ++rspIndex) {
                    IAttributeDictionaryPtr attributes;
                    auto updateAttributes = [&] (const TErrorOr<TIntrusivePtr<TYPathProxy::TRspGet>>& rspOrError) {
                        if (!rspOrError.IsOK()) {
                            return;
                        }

                        try {
                            auto responseAttributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
                            if (attributes) {
                                attributes->MergeFrom(*responseAttributes);
                            } else {
                                attributes = std::move(responseAttributes);
                            }
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Error parsing revival attributes of operation")
                                << TErrorAttribute("operation_id", operation->GetId())
                                << ex;
                        }
                    };

                    updateAttributes(rsps[rspIndex]);

                    // Commit transaction may be missing or aborted.
                    if (!attributes) {
                        continue;
                    }

                    if (attributes->Get<bool>(CommittedAttribute, false)) {
                        revivalDescriptor.OperationCommitted = true;
                        // If it is an output transaction, it should be committed. It is exactly when there are
                        // two responses and we are processing the first one (cf. previous for-loop).
                        if (rspIndex == 0 && operation->Transactions()->OutputTransaction) {
                            revivalDescriptor.ShouldCommitOutputTransaction = true;
                        }
                        break;
                    }
                }
            }
        }

        YT_LOG_INFO("Revival descriptors fetched (OperationCount: %v)",
            operations.size());
    }


    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTagSentinel,
        int subbatchSize = 100)
    {
        TObjectServiceProxy proxy(
            Bootstrap_->GetClient(),
            channelKind,
            cellTag,
            /*stickyGroupSizeCache*/ nullptr);
        auto batchReq = proxy.ExecuteBatch(subbatchSize);
        YT_VERIFY(LockTransaction_);
        auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
        ToProto(prerequisiteTransaction->mutable_transaction_id(), LockTransaction_->GetId());
        return batchReq;
    }


    void DoCleanup()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LockTransaction_.Reset();

        StopPeriodicActivities();

        if (CancelableContext_) {
            CancelableContext_->Cancel(TError(EErrorCode::MasterDisconnected, "Master disconnected"));
            CancelableContext_.Reset();
        }

        std::fill(CancelableControlInvokers_.begin(), CancelableControlInvokers_.end(), nullptr);

        State_.store(EMasterConnectorState::Disconnected);
        ConnectionTime_.store(TInstant::Zero());
    }

    void DoDisconnect(const TError& error) noexcept
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        if (State_ == EMasterConnectorState::Connected) {
            YT_LOG_WARNING(error, "Disconnecting master");
            MasterDisconnected_.Fire();
            YT_LOG_WARNING("Master disconnected");
        }

        DoCleanup();
        StartConnecting(true);
    }

    void StartPeriodicActivities()
    {
        OperationNodesUpdateExecutor_->Start();

        CommonWatchersExecutor_->Start();

        AlertsExecutor_->Start();

        for (const auto& executor : CustomWatcherExecutors_) {
            YT_VERIFY(executor);
            executor->Start();
        }
    }

    void StopPeriodicActivities()
    {
        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->Stop();
            OperationNodesUpdateExecutor_.Reset();
        }

        if (CommonWatchersExecutor_) {
            YT_UNUSED_FUTURE(CommonWatchersExecutor_->Stop());
            CommonWatchersExecutor_.Reset();
        }

        if (AlertsExecutor_) {
            YT_UNUSED_FUTURE(AlertsExecutor_->Stop());
            AlertsExecutor_.Reset();
        }

        for (auto& executor : CustomWatcherExecutors_) {
            if (executor) {
                YT_UNUSED_FUTURE(executor->Stop());
            }
            executor.Reset();
        }
    }

    void OnOperationUpdateFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!error.IsOK());

        Disconnect(TError("Failed to update operation node") << error);
    }

    void DoUpdateOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            operation->SetShouldFlush(false);

            auto batchReq = StartObjectBatchRequest();
            GenerateMutationId(batchReq);

            auto operationPath = GetOperationPath(operation->GetId());

            if (operation->GetShouldFlushAcl()) {
                auto aclBatchReq = StartObjectBatchRequest();
                auto req = TYPathProxy::Set(GetOperationPath(operation->GetId()) + "/@acl");
                auto operationNodeAcl = MakeOperationArtifactAcl(operation->GetRuntimeParameters()->Acl);
                req->set_value(ConvertToYsonStringNestingLimited(operationNodeAcl).ToString());
                aclBatchReq->AddRequest(req, "set_acl");

                auto aclBatchRspOrError = WaitFor(aclBatchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(aclBatchRspOrError);

                auto rspOrErr = aclBatchRspOrError.Value()->GetResponse("set_acl");
                auto scheduler = Bootstrap_->GetScheduler();
                if (!rspOrErr.IsOK()) {
                    auto error = TError("Failed to set operation ACL")
                        << TErrorAttribute("operation_id", operation->GetId())
                        << rspOrErr;
                    WaitFor(scheduler->SetOperationAlert(operation->GetId(), EOperationAlertType::InvalidAcl, error))
                        .ThrowOnError();
                    YT_LOG_INFO(error);
                } else {
                    WaitFor(scheduler->SetOperationAlert(operation->GetId(), EOperationAlertType::InvalidAcl, TError()))
                        .ThrowOnError();
                }
            }

            auto multisetReq = TYPathProxy::MultisetAttributes(operationPath + "/@");

            // Set suspended flag.
            {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("suspended");
                req->set_value(ConvertToYsonStringNestingLimited(operation->GetSuspended()).ToString());
            }

            // Set events.
            {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("events");
                req->set_value(ConvertToYsonStringNestingLimited(operation->Events()).ToString());
            }

            // Set result.
            if (operation->IsFinishedState()) {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("result");
                req->set_value(ConvertToYsonStringNestingLimited(operation->BuildResultString()).ToString());
            }

            // Set end time, if given.
            if (operation->GetFinishTime()) {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("finish_time");
                req->set_value(ConvertToYsonStringNestingLimited(*operation->GetFinishTime()).ToString());
            }

            // Set state.
            {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("state");
                req->set_value(ConvertToYsonStringNestingLimited(operation->GetState()).ToString());
            }

            // Set alerts.
            {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("alerts");
                req->set_value(ConvertToYsonStringNestingLimited(operation->BuildAlertsString()).ToString());
            }

            // Set slot index per pool tree.
            {
                auto req = multisetReq->add_subrequests();
                req->set_attribute("slot_index_per_pool_tree");
                req->set_value(ConvertToYsonStringNestingLimited(operation->GetSlotIndices()).ToString());
            }

            // Set runtime parameters.
            {
                bool enableHeavyRuntimeParameters = Config_->EnableHeavyRuntimeParameters;
                auto req = multisetReq->add_subrequests();
                req->set_attribute("runtime_parameters");
                auto valueYson = BuildYsonStringFluently()
                    .Value(operation->GetRuntimeParameters(), /*serializeHeavy*/ !enableHeavyRuntimeParameters);
                ValidateYson(valueYson, GetYsonNestingLevelLimit());
                req->set_value(valueYson.ToString());

                if (enableHeavyRuntimeParameters) {
                    auto reqHeavy = multisetReq->add_subrequests();
                    reqHeavy->set_attribute("heavy_runtime_parameters");
                    auto valueYson = BuildYsonStringFluently()
                        .DoMap([&] (auto fluent) {
                            SerializeHeavyRuntimeParameters(fluent, *operation->GetRuntimeParameters());
                        });
                    ValidateYson(valueYson, GetYsonNestingLevelLimit());
                    reqHeavy->set_value(valueYson.ToString());
                }
            }

            batchReq->AddRequest(multisetReq, "update_op_node");

            operation->SetShouldFlushAcl(false);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

            YT_LOG_DEBUG("Operation node updated (OperationId: %v)", operation->GetId());
        } catch (const std::exception& ex) {
            auto error = TError("Error updating operation node %v",
                operation->GetId())
                << ex;
            if (IsMasterDisconnectionError(error)) {
                error.SetCode(EErrorCode::MasterDisconnected);
            }
            THROW_ERROR error;
        }
    }

    TCallback<TFuture<void>()> UpdateOperationNode(TOperationId /*operationId*/, TOperationNodeUpdate* update)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // If operation is starting the node of operation may be missing.
        if (update->Operation->GetState() == EOperationState::Starting) {
            return {};
        }

        if (!update->Operation->GetShouldFlush() && !update->Operation->GetShouldFlushAcl()) {
            return {};
        }

        return BIND(&TImpl::DoUpdateOperationNode,
            MakeStrong(this),
            update->Operation)
            .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector));
    }

    void OnInitializedOperationNodeUpdated(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();
        auto error = GetCumulativeError(batchRspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error updating initialized operation node %v",
            operationId);

        YT_LOG_INFO("Initialized operation node updated (OperationId: %v)",
            operationId);
    }

    ITransactionPtr StartWatcherLockTransaction(const TCustomWatcherRecord& watcher)
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler %Qlv watcher lock at %v",
            watcher.WatcherType,
            GetDefaultAddress(Bootstrap_->GetLocalAddresses())));
        TTransactionStartOptions options{
            .Timeout = watcher.LockOptions->WaitTimeout,
            .AutoAbort = true,
            .Ping = false,
            .Attributes = std::move(attributes),
        };

        auto transactionOrError = WaitFor(LockTransaction_->StartTransaction(ETransactionType::Master, options));

        if (!transactionOrError.IsOK()) {
            THROW_ERROR transactionOrError.Wrap("Failed to start lock transaction for watcher")
                << TErrorAttribute("watcher_type", watcher.WatcherType);
        }

        YT_LOG_INFO("Watcher lock transaction created (WatcherType: %v, TransactionId: %v)",
            watcher.WatcherType,
            transactionOrError.Value()->GetId());

        return transactionOrError.Value();
    }

    void ExecuteCustomWatcherUpdate(const TCustomWatcherRecord& watcher, bool strictMode)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

        ITransactionPtr watcherLockTransaction;
        if (watcher.LockOptions) {
            try {
                watcherLockTransaction = StartWatcherLockTransaction(watcher);
                LockNodeWithWait(
                    Bootstrap_->GetClient(),
                    watcherLockTransaction,
                    watcher.LockOptions->LockPath,
                    watcher.LockOptions->CheckBackoff,
                    watcher.LockOptions->WaitTimeout);
            } catch (const std::exception& ex) {
                HandleWatcherError(
                    TError("Watcher failed to take lock")
                        << ex
                        << TErrorAttribute("watcher_type", watcher.WatcherType),
                    strictMode,
                    watcher.AlertType);
                return;
            }

            YT_LOG_INFO("Lock for watcher acquired (WatcherType: %v)", watcher.WatcherType);

            TPrerequisiteOptions prerequisiteOptions;
            prerequisiteOptions.PrerequisiteTransactionIds.push_back(watcherLockTransaction->GetId());
            SetPrerequisites(batchReq, prerequisiteOptions);
        }

        watcher.Requester(batchReq);
        auto batchRspOrError = WaitFor(batchReq->Invoke());
        if (!batchRspOrError.IsOK()) {
            HandleWatcherError(
                batchRspOrError.Wrap("Watcher batch request failed")
                    << TErrorAttribute("watcher_type", watcher.WatcherType),
                strictMode,
                watcher.AlertType);
            return;
        }
        if (watcherLockTransaction) {
            YT_UNUSED_FUTURE(watcherLockTransaction->Abort());
        }

        RunWatcherHandler(watcher, batchRspOrError.Value(), strictMode);
    }

    void UpdateWatchers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ == EMasterConnectorState::Connected);

        YT_LOG_DEBUG("Updating watchers");

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
        for (const auto& watcher : CommonWatcherRecords_) {
            watcher.Requester(batchReq);
        }
        Y_UNUSED(WaitFor(batchReq->Invoke().Apply(
            BIND(&TImpl::OnCommonWatchersUpdated, MakeStrong(this))
                .AsyncVia(GetCancelableControlInvoker(EControlQueue::CommonPeriodicActivity)))));
    }

    void OnCommonWatchersUpdated(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ == EMasterConnectorState::Connected);

        if (!batchRspOrError.IsOK()) {
            YT_LOG_WARNING(batchRspOrError, "Error updating common watchers");
            return;
        }

        const auto& batchRsp = batchRspOrError.Value();
        for (const auto& watcher : CommonWatcherRecords_) {
            RunWatcherHandler(watcher, batchRsp, /*strictMode*/ false);
        }

        YT_LOG_DEBUG("Common watchers updated");
    }

    void RunWatcherHandler(const TWatcherRecord& watcher, TObjectServiceProxy::TRspExecuteBatchPtr responses, bool strictMode)
    {
        try {
            watcher.Handler(responses);
            if (watcher.AlertType) {
                SetSchedulerAlert(*watcher.AlertType, TError());
            }
        } catch (const TErrorException& ex) {
            if (ex.Error().GetCode() != EErrorCode::WatcherHandlerFailed) {
                throw;
            }
            HandleWatcherError(ex.Error(), strictMode, watcher.AlertType);
        }
    }

    void HandleWatcherError(const TError& error, bool strictMode, std::optional<ESchedulerAlertType> alertType)
    {
        if (strictMode) {
            SetSchedulerAlert(ESchedulerAlertType::SchedulerCannotConnect, error);
            UpdateAlerts();
            THROW_ERROR(error);
        }

        if (alertType) {
            SetSchedulerAlert(*alertType, error);
        }
        YT_LOG_WARNING(error);
    }

    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TError> alerts;
        for (auto alertType : TEnumTraits<ESchedulerAlertType>::GetDomainValues()) {
            const auto& alert = Alerts_[alertType];
            if (!alert.IsOK()) {
                alerts.push_back(alert);
            }
        }

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto req = TYPathProxy::Set("//sys/scheduler/@alerts");
        req->set_value(ConvertToYsonStringNestingLimited(alerts).ToString());

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error updating scheduler alerts");
        }
    }

    void OnClusterDirectorySynchronized(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SetSchedulerAlert(ESchedulerAlertType::SyncClusterDirectory, error);
    }

    void UpdateLockTransactionTimeout(TDuration timeout)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(LockTransaction_);
        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto req = TYPathProxy::Set(FromObjectId(LockTransaction_->GetId()) + "/@timeout");
        req->set_value(ConvertToYsonStringNestingLimited(timeout.MilliSeconds()).ToString());
        auto rspOrError = WaitFor(proxy.Execute(req));

        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_WARNING(rspOrError, "Error updating lock transaction timeout (TransactionId: %v)",
                    LockTransaction_->GetId());
            } else {
                THROW_ERROR_EXCEPTION("Error updating lock transaction timeout")
                    << rspOrError
                    << TErrorAttribute("transaction_id", LockTransaction_->GetId());
            }
            return;
        }

        YT_LOG_DEBUG("Lock transaction timeout updated (TransactionId: %v, Timeout: %v)",
            LockTransaction_->GetId(),
            timeout.MilliSeconds());
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TMasterConnector::~TMasterConnector() = default;

void TMasterConnector::Start()
{
    Impl_->Start();
}

EMasterConnectorState TMasterConnector::GetState() const
{
    return Impl_->GetState();
}

TInstant TMasterConnector::GetConnectionTime() const
{
    return Impl_->GetConnectionTime();
}

const NApi::ITransactionPtr& TMasterConnector::GetLockTransaction() const
{
    return Impl_->GetLockTransaction();
}

void TMasterConnector::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
}

const IInvokerPtr& TMasterConnector::GetCancelableControlInvoker(EControlQueue queue) const
{
    return Impl_->GetCancelableControlInvoker(queue);
}

void TMasterConnector::RegisterOperation(const TOperationPtr& operation)
{
    Impl_->RegisterOperation(operation);
}

void TMasterConnector::UnregisterOperation(const TOperationPtr& operation)
{
    Impl_->UnregisterOperation(operation);
}

TFuture<void> TMasterConnector::CreateOperationNode(const TOperationPtr& operation)
{
    return Impl_->CreateOperationNode(operation);
}

TFuture<void> TMasterConnector::UpdateInitializedOperationNode(const TOperationPtr& operation)
{
    return Impl_->UpdateInitializedOperationNode(operation);
}

TFuture<void> TMasterConnector::FlushOperationNode(const TOperationPtr& operation)
{
    return Impl_->FlushOperationNode(operation);
}

TFuture<void> TMasterConnector::FetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
{
    return Impl_->FetchOperationRevivalDescriptors(operations);
}

TFuture<TYsonString> TMasterConnector::GetOperationNodeProgressAttributes(const TOperationPtr& operation)
{
    return Impl_->GetOperationNodeProgressAttributes(operation);
}

TFuture<void> TMasterConnector::CheckTransactionAlive(TTransactionId transactionId)
{
    return Impl_->CheckTransactionAlive(transactionId);
}

void TMasterConnector::InvokeStoringStrategyState(TPersistentStrategyStatePtr strategyState)
{
    Impl_->InvokeStoringStrategyState(std::move(strategyState));
}

TFuture<void> TMasterConnector::UpdateLastMeteringLogTime(TInstant time)
{
    return Impl_->UpdateLastMeteringLogTime(time);
}

void TMasterConnector::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    Impl_->SetSchedulerAlert(alertType, alert);
}

void TMasterConnector::UpdateConfig(const TSchedulerConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

void TMasterConnector::AddCommonWatcher(
    TWatcherRequester requester,
    TWatcherHandler handler,
    std::optional<ESchedulerAlertType> alertType)
{
    Impl_->AddCommonWatcher(std::move(requester), std::move(handler), alertType);
}

void TMasterConnector::SetCustomWatcher(
    EWatcherType type,
    TWatcherRequester requester,
    TWatcherHandler handler,
    TDuration period,
    std::optional<ESchedulerAlertType> alertType,
    std::optional<TWatcherLockOptions> lockOptions)
{
    Impl_->SetCustomWatcher(type, std::move(requester), std::move(handler), period, alertType, lockOptions);
}

DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnecting, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterHandshake, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnected, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
