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

#include <yt/yt/server/lib/misc/update_executor.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/job_resources_serialize.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/actions/cancelable_context.h>

namespace NYT::NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NRpc;
using namespace NApi;
using namespace NSecurityClient;
using namespace NConcurrency;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::GetDefaultAddress;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("MasterConnector");

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
            ->GetMasterClient()
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

                auto operationYson = BuildYsonStringFluently()
                    .BeginAttributes()
                        .Do(BIND(&BuildMinimalOperationAttributes, operation))
                        .Item("opaque").Value(true)
                        .Item("runtime_parameters").Value(operation->GetRuntimeParameters())
                    .EndAttributes()
                    .BeginMap()
                        .Item("jobs").BeginAttributes()
                            .Item("opaque").Value(true)
                            .Item("acl").Value(MakeOperationArtifactAcl(operation->GetRuntimeParameters()->Acl))
                            .Item("inherit_acl").Value(false)
                        .EndAttributes()
                        .BeginMap().EndMap()
                    .EndMap()
                    .ToString();

                auto req = TYPathProxy::Set(GetOperationPath(operationId));
                req->set_value(operationYson);
                req->set_recursive(true);
                req->set_force(true);
                GenerateMutationId(req);
                batchReq->AddRequest(req);

                auto batchRspOrError = WaitFor(batchReq->Invoke());

                GetCumulativeError(batchRspOrError)
                    .ThrowOnError();
            }


            if (operation->GetSecureVault()) {
                auto batchReq = StartObjectBatchRequest();

                // Create secure vault.
                auto attributes = CreateEphemeralAttributes();
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
            THROW_ERROR_EXCEPTION("Error creating operation node %v", operationId)
                << ex;
        }

        YT_LOG_INFO("Operation node created (OperationId: %v)",
            operationId);
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

        auto attributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Do(BIND(&BuildFullOperationAttributes, operation))
                .Item("brief_spec").Value(operation->BriefSpecString())
            .EndMap());

        auto req = TYPathProxy::Multiset(GetOperationPath(operationId) + "/@");
        GenerateMutationId(req);
        for (const auto& [key, value] : attributes->ListPairs()) {
            auto* subrequest = req->add_subrequests();
            subrequest->set_key(key);
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

        YT_LOG_INFO("Storing persistent strategy state");

        auto batchReq = StartObjectBatchRequest();

        auto req = NCypressClient::TCypressYPathProxy::Create(StrategyStatePath);
        req->set_type(static_cast<int>(EObjectType::Document));
        req->set_force(true);

        auto* attribute = req->mutable_node_attributes()->add_attributes();
        attribute->set_key("value");
        attribute->set_value(ConvertToYsonString(persistentStrategyState, EYsonFormat::Binary).ToString());

        GenerateMutationId(req);
        batchReq->AddRequest(req);

        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_ERROR(rspOrError, "Error storing persistent strategy state");
        } else {
            YT_LOG_INFO("Persistent strategy state successfully stored");
        }
    }

    void InvokeStoringSchedulingSegmentsState(TPersistentSchedulingSegmentsStatePtr segmentsState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        GetCancelableControlInvoker(EControlQueue::MasterConnector)
            ->Invoke(BIND(&TImpl::StoreSchedulingSegmentsState, MakeStrong(this), Passed(std::move(segmentsState))));
    }

    void StoreSchedulingSegmentsState(const TPersistentSchedulingSegmentsStatePtr& persistentSegmentsState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ != EMasterConnectorState::Disconnected);

        YT_LOG_INFO("Storing persistent scheduling segments state");

        auto batchReq = StartObjectBatchRequest();

        auto req = NCypressClient::TCypressYPathProxy::Create(SegmentsStatePath);
        req->set_type(static_cast<int>(EObjectType::Document));
        req->set_force(true);

        auto* attribute = req->mutable_node_attributes()->add_attributes();
        attribute->set_key("value");
        attribute->set_value(ConvertToYsonString(persistentSegmentsState, EYsonFormat::Binary).ToString());

        GenerateMutationId(req);
        batchReq->AddRequest(req);

        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error storing persistent scheduling segments state");
        } else {
            YT_LOG_INFO("Persistent scheduling segments state successfully stored");
        }
    }

    void AttachJobContext(
        const TYPath& path,
        TChunkId chunkId,
        TOperationId operationId,
        TJobId jobId,
        const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            TJobFile file{
                jobId,
                path,
                chunkId,
                "input_context"
            };
            auto client = Bootstrap_->GetMasterClient()->GetNativeConnection()->CreateNativeClient(TClientOptions::FromUser(user));
            SaveJobFiles(client, operationId, { file });
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error saving input context for job %v into %v", jobId, path)
                << ex;
        }
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
        std::optional<ESchedulerAlertType> alertType)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CustomWatcherRecords_[type] = TCustomWatcherRecord{{std::move(requester), std::move(handler), alertType}, type, period};
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (State_ == EMasterConnectorState::Connected &&
            Config_->LockTransactionTimeout != config->LockTransactionTimeout)
        {
            BIND(&TImpl::UpdateLockTransactionTimeout, MakeStrong(this), config->LockTransactionTimeout)
                .AsyncVia(GetCancelableControlInvoker(EControlQueue::MasterConnector))
                .Run();
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
    TEnumIndexedVector<EControlQueue, IInvokerPtr> CancelableControlInvokers_;

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
    };

    std::vector<TWatcherRecord> CommonWatcherRecords_;

    TEnumIndexedVector<EWatcherType, TCustomWatcherRecord> CustomWatcherRecords_;
    TEnumIndexedVector<EWatcherType, TPeriodicExecutorPtr> CustomWatcherExecutors_;

    TEnumIndexedVector<ESchedulerAlertType, TError> Alerts_;

    struct TOperationNodeUpdate
    {
        explicit TOperationNodeUpdate(TOperationPtr operation)
            : Operation(std::move(operation))
        { }

        TOperationPtr Operation;
    };

    TIntrusivePtr<TUpdateExecutor<TOperationId, TOperationNodeUpdate>> OperationNodesUpdateExecutor_;

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
                BIND(&TImpl::ExecuteCustomWatcherUpdate, MakeWeak(this), record),
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
            SyncClusterDirectory();
            SyncMediumDirectory();
            ListOperations();
            RequestOperationAttributes();
            SubmitOperationsToCleaner();
            RequestSchedulingSegmentsState();
            FireHandshake();
        }

    private:
        const TIntrusivePtr<TImpl> Owner_;
        const TAddressMap ServiceAddresses_;

        std::vector<TOperationId> OperationIds_;
        std::vector<TOperationId> OperationIdsToArchive_;
        std::vector<TOperationId> OperationIdsToRemove_;

        TMasterHandshakeResult Result_;

        void FireConnecting()
        {
            Owner_->MasterConnecting_.Fire();
        }

        void EnsureNoSafeMode()
        {
            TObjectServiceProxy proxy(Owner_
                ->Bootstrap_
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Follower));

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
            TObjectServiceProxy proxy(Owner_
                ->Bootstrap_
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
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
                req->set_value(ConvertToYsonString(Owner_->Bootstrap_->GetConfig()->CypressAnnotations).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Create(path + "/orchid");
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::Orchid));
                auto attributes = CreateEphemeralAttributes();
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

            auto client = Owner_->Bootstrap_->GetMasterClient();
            auto transactionOrError = WaitFor(Owner_->Bootstrap_->GetMasterClient()->StartTransaction(
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
                req->set_value(ConvertToYsonString(addresses).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid&/@remote_addresses");
                req->set_value(ConvertToYsonString(addresses).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@connection_time");
                req->set_value(ConvertToYsonString(TInstant::Now()).ToString());
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
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetClusterDirectorySynchronizer()
                ->Sync(/* force */ true))
                .ThrowOnError();
            YT_LOG_INFO("Sync cluster directory finished");
        }

        void SyncMediumDirectory()
        {
            YT_LOG_INFO("Sync medium directory started");
            WaitFor(Owner_
                ->Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetMediumDirectorySynchronizer()
                ->NextSync(/* force */ true))
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
                PrimaryMasterCellTag,
                /* subbatchSize */ 100);

            auto listOperationsResult = NScheduler::ListOperations(createBatchRequest);
            OperationIds_.reserve(listOperationsResult.OperationsToRevive.size());

            for (const auto& [operationId, state] : listOperationsResult.OperationsToRevive) {
                YT_LOG_DEBUG("Found operation in Cypress (OperationId: %v, State: %v)",
                    operationId,
                    state);
                OperationIds_.push_back(operationId);
            }

            OperationIdsToArchive_ = std::move(listOperationsResult.OperationsToArchive);
            OperationIdsToRemove_ = std::move(listOperationsResult.OperationsToRemove);

            YT_LOG_INFO("Finished listing existing operations");
        }

        struct TOperationDataToParse final
        {
            TYsonString AttributesYson;
            TYsonString SecureVaultYson;
            TOperationId OperationId;
        };

        std::vector<TOperationPtr> ParseOperationsBatch(
            const std::vector<TOperationDataToParse>& rspValuesChunk,
            const int parseOperationAttributesBatchSize,
            const bool skipOperationsWithMalformedSpecDuringRevival)
        {
            std::vector<TOperationPtr> result;
            result.reserve(parseOperationAttributesBatchSize);

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
                }

                try {
                    if (attributesNode->Get<bool>("banned", false)) {
                        YT_LOG_INFO("Operation manually banned (OperationId: %v)", rspValues.OperationId);
                        continue;
                    }
                    auto operation = TryCreateOperationFromAttributes(
                        rspValues.OperationId,
                        *attributesNode,
                        secureVault);
                    result.push_back(operation);
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
                "output_completion_transaction_id",
                "suspended",
                "erased_trees",
                "banned",
                "initial_aggregated_min_needed_resources",
                "registration_index",
            };
            const int operationsCount = static_cast<int>(OperationIds_.size());

            YT_LOG_INFO("Fetching attributes and secure vaults for unfinished operations (UnfinishedOperationCount: %v)",
                operationsCount);

            auto batchReq = Owner_->StartObjectBatchRequest(
                EMasterChannelKind::Follower,
                PrimaryMasterCellTag,
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

                std::vector<TFuture<std::vector<TOperationPtr>>> futures;
                futures.reserve(RoundUp(operationsCount, chunkSize));

                for (auto startIndex = 0; startIndex < operationsCount; startIndex += chunkSize) {
                    std::vector<TOperationDataToParse> operationsDataToParseBatch;

                    operationsDataToParseBatch.reserve(chunkSize);
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

                        operationsDataToParseBatch.push_back({std::move(atttibutesNodeStr), std::move(secureVaultYson), operationId});
                    }

                    futures.push_back(BIND(
                            &TRegistrationPipeline::ParseOperationsBatch,
                            MakeStrong(this),
                            std::move(operationsDataToParseBatch),
                            chunkSize,
                            Owner_->Config_->SkipOperationsWithMalformedSpecDuringRevival
                        )
                        .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
                        .Run()
                    );
                }
                YT_LOG_INFO("Operation attributes batches for parsing formed");

                Result_.Operations.reserve(OperationIds_.size());
                auto result = WaitFor(AllSucceeded(futures)).ValueOrThrow();

                for (auto& chunk : result) {
                    for (auto& operation : chunk) {
                        Result_.Operations.push_back(std::move(operation));
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

            YT_LOG_INFO("Operation objects created from attributes");
        }

        TOperationPtr TryCreateOperationFromAttributes(
            TOperationId operationId,
            const IAttributeDictionary& attributes,
            const IMapNodePtr& secureVault)
        {
            auto specString = attributes.GetYson("spec");
            auto specNode = ConvertSpecStringToNode(specString);
            TPreprocessedSpec preprocessedSpec;
            ParseSpec(std::move(specNode), /* specTemplate */ nullptr, operationId, &preprocessedSpec);
            preprocessedSpec.ExperimentAssignments =
                attributes.Get<std::vector<TExperimentAssignmentPtr>>("experiment_assignments", {});
            const auto& spec = preprocessedSpec.Spec;

            // NB: Keep stuff below in sync with #RequestOperationAttributes.
            auto user = attributes.Get<TString>("authenticated_user");

            YT_VERIFY(attributes.Contains("runtime_parameters"));
            auto runtimeParameters = attributes.Get<TOperationRuntimeParametersPtr>("runtime_parameters");
            if (spec->AddAuthenticatedUserToAcl) {
                TSerializableAccessControlEntry ace(
                    ESecurityAction::Allow,
                    std::vector<TString>{user},
                    EPermissionSet(EPermission::Read | EPermission::Manage));
                auto it = std::find(runtimeParameters->Acl.Entries.begin(), runtimeParameters->Acl.Entries.end(), ace);
                if (it == runtimeParameters->Acl.Entries.end()) {
                    runtimeParameters->Acl.Entries.push_back(std::move(ace));
                }
            }

            // COMPAT(gritukan)
            auto annotations = attributes.Find<IMapNodePtr>("annotations");
            if (annotations) {
                runtimeParameters->Annotations = annotations;
            }

            auto scheduler = Owner_->Bootstrap_->GetScheduler();
            auto operation = New<TOperation>(
                operationId,
                attributes.Get<EOperationType>("operation_type"),
                attributes.Get<TMutationId>("mutation_id"),
                attributes.Get<TTransactionId>("user_transaction_id"),
                spec,
                std::move(preprocessedSpec.CustomSpecPerTree),
                std::move(preprocessedSpec.SpecString),
                secureVault,
                runtimeParameters,
                scheduler->GetOperationBaseAcl(),
                user,
                attributes.Get<TInstant>("start_time"),
                Owner_->GetCancelableControlInvoker(EControlQueue::Operation),
                spec->Alias,
                std::move(preprocessedSpec.ExperimentAssignments),
                attributes.Get<EOperationState>("state"),
                attributes.Get<std::vector<TOperationEvent>>("events", {}),
                attributes.Get<bool>("suspended", false),
                attributes.Find<TJobResources>("initial_aggregated_min_needed_resources"),
                attributes.Get<int>("registration_index", 0));


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

        void StrictUpdateWatchers()
        {
            YT_LOG_INFO("Request watcher updates");
            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (const auto& watcher : Owner_->CommonWatcherRecords_) {
                watcher.Requester.Run(batchReq);
            }
            for (const auto& watcher : Owner_->CustomWatcherRecords_) {
                watcher.Requester.Run(batchReq);
            }

            auto watcherResponses = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            YT_LOG_INFO("Handling watcher update results");

            for (const auto& watcher : Owner_->CommonWatcherRecords_) {
                Owner_->RunWatcherHandler(watcher, watcherResponses, /* strictMode */ true);
            }

            for (const auto& watcher : Owner_->CustomWatcherRecords_) {
                Owner_->RunWatcherHandler(watcher, watcherResponses, /* strictMode */ true);
            }

            YT_LOG_INFO("Watchers update results handled");
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

        void SubmitOperationsToCleaner()
        {
            YT_LOG_INFO("Submitting operations to cleaner (ArchiveCount: %v, RemoveCount: %v)",
                OperationIdsToArchive_.size(),
                OperationIdsToRemove_.size());

            const auto& operationsCleaner = Owner_->Bootstrap_->GetScheduler()->GetOperationsCleaner();

            for (auto operationId : OperationIdsToRemove_) {
                operationsCleaner->SubmitForRemoval({operationId});
            }

            auto createBatchRequest = BIND(
                &TImpl::StartObjectBatchRequest,
                Owner_,
                EMasterChannelKind::Follower,
                PrimaryMasterCellTag,
                Owner_->Config_->FetchOperationAttributesSubbatchSize);

            auto operations = FetchOperationsFromCypressForCleaner(
                OperationIdsToArchive_,
                createBatchRequest,
                Owner_->Config_->OperationsCleaner->ParseOperationAttributesBatchSize);

            for (auto& operation : operations) {
                operationsCleaner->SubmitForArchivation(std::move(operation));
            }

            YT_LOG_INFO("Operations submitted to cleaner");
        }

        void RequestSchedulingSegmentsState()
        {
            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            batchReq->AddRequest(TYPathProxy::Get(SegmentsStatePath), "get_scheduling_segments_state");

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_scheduling_segments_state");
            if (!rspOrError.IsOK() && !rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_WARNING(rspOrError, "Error fetching scheduling segments state");
                return;
            }

            if (rspOrError.IsOK()) {
                auto value = rspOrError.ValueOrThrow()->value();
                try {
                    Result_.SchedulingSegmentsState = ConvertTo<TPersistentSchedulingSegmentsStatePtr>(TYsonString(value));
                    YT_LOG_INFO("Successfully fetched strategy state");
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to deserialize scheduling segments state, ignoring it");
                    Result_.SchedulingSegmentsState.Reset();
                }
            }
        }
    };

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
                PrimaryMasterCellTag,
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

            for (const auto& operation : operations) {
                auto operationId = operation->GetId();

                auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                    "get_op_attr_" + ToString(operationId))
                    .ValueOrThrow();

                auto attributes = ConvertToAttributes(TYsonString(attributesRsp->value()));

                auto attachTransaction = [&] (TTransactionId transactionId, bool ping, const TString& name = TString()) -> ITransactionPtr {
                    if (!transactionId) {
                        if (name) {
                            YT_LOG_DEBUG("Missing %v transaction (OperationId: %v, TransactionId: %v)",
                                name,
                                operationId,
                                transactionId);
                        }
                        return nullptr;
                    }
                    try {
                        auto client = Bootstrap_->GetRemoteMasterClient(CellTagFromId(transactionId));

                        TTransactionAttachOptions options;
                        options.PingPeriod = Config_->OperationTransactionPingPeriod;
                        options.Ping = ping;
                        options.PingAncestors = false;
                        return client->AttachTransaction(transactionId, options);
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
        }

        YT_LOG_INFO("Fetching committed flags (OperationCount: %v)",
            operations.size());

        {
            std::vector<TOperationPtr> operationsToRevive;

            auto getBatchKey = [] (const TOperationPtr& operation) {
                return "get_op_committed_attr_" + ToString(operation->GetId());
            };

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

            for (const auto& operation : operations) {
                const auto& transactions = *operation->Transactions();
                std::vector<TTransactionId> possibleTransactions;
                if (transactions.OutputTransaction) {
                    possibleTransactions.push_back(transactions.OutputTransaction->GetId());
                }
                if (operation->GetUserTransactionId()) {
                    possibleTransactions.push_back(operation->GetUserTransactionId());
                }
                possibleTransactions.push_back(NullTransactionId);

                operationsToRevive.push_back(operation);

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

            for (const auto& operation : operationsToRevive) {
                auto& revivalDescriptor = *operation->RevivalDescriptor();
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(operation));

                for (size_t rspIndex = 0; rspIndex < rsps.size(); ++rspIndex) {
                    IAttributeDictionaryPtr attributes;
                    auto updateAttributes = [&] (const TErrorOr<TIntrusivePtr<TYPathProxy::TRspGet>>& rspOrError) {
                        if (!rspOrError.IsOK()) {
                            return;
                        }

                        auto responseAttributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
                        if (attributes) {
                            attributes->MergeFrom(*responseAttributes);
                        } else {
                            attributes = std::move(responseAttributes);
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
    }


    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTag,
        int subbatchSize = 100)
    {
        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(channelKind, cellTag));
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
            CommonWatchersExecutor_->Stop();
            CommonWatchersExecutor_.Reset();
        }

        if (AlertsExecutor_) {
            AlertsExecutor_->Stop();
            AlertsExecutor_.Reset();
        }

        for (auto& executor : CustomWatcherExecutors_) {
            if (executor) {
                executor->Stop();
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

            // Set "jobs" node ACL.
            if (operation->GetShouldFlushAcl()) {
                auto aclBatchReq = StartObjectBatchRequest();
                auto req = TYPathProxy::Set(GetJobsPath(operation->GetId()) + "/@acl");
                auto operationNodeAcl = MakeOperationArtifactAcl(operation->GetRuntimeParameters()->Acl);
                req->set_value(ConvertToYsonString(operationNodeAcl).ToString());
                aclBatchReq->AddRequest(req, "set_acl");

                auto aclBatchRspOrError = WaitFor(aclBatchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(aclBatchRspOrError);

                auto rspOrErr = aclBatchRspOrError.Value()->GetResponse("set_acl");
                if (!rspOrErr.IsOK()) {
                    auto error = TError("Failed to set operation ACL")
                        << TErrorAttribute("operation_id", operation->GetId())
                        << rspOrErr;
                    operation->SetAlert(EOperationAlertType::InvalidAcl, error);
                    YT_LOG_INFO(error);
                } else {
                    operation->ResetAlert(EOperationAlertType::InvalidAcl);
                }
            }

            auto multisetReq = TYPathProxy::Multiset(operationPath + "/@");

            // Set suspended flag.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("suspended");
                req->set_value(ConvertToYsonString(operation->GetSuspended()).ToString());
            }

            // Set events.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("events");
                req->set_value(ConvertToYsonString(operation->Events()).ToString());
            }

            // Set result.
            if (operation->IsFinishedState()) {
                auto req = multisetReq->add_subrequests();
                req->set_key("result");
                req->set_value(operation->BuildResultString().ToString());
            }

            // Set end time, if given.
            if (operation->GetFinishTime()) {
                auto req = multisetReq->add_subrequests();
                req->set_key("finish_time");
                req->set_value(ConvertToYsonString(*operation->GetFinishTime()).ToString());
            }

            // Set state.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("state");
                req->set_value(ConvertToYsonString(operation->GetState()).ToString());
            }

            // Set alerts.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("alerts");
                req->set_value(operation->BuildAlertsString().ToString());
            }

            // Set runtime parameters.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("runtime_parameters");
                req->set_value(ConvertToYsonString(operation->GetRuntimeParameters()).ToString());
            }

            // Set initial aggregated min needed resources.
            if (auto initialMinNeededResources = operation->GetInitialAggregatedMinNeededResources()) {
                auto req = multisetReq->add_subrequests();
                req->set_key("initial_aggregated_min_needed_resources");
                req->set_value(ConvertToYsonString(*initialMinNeededResources).ToString());
            }

            batchReq->AddRequest(multisetReq, "update_op_node");

            operation->SetShouldFlushAcl(false);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

            YT_LOG_DEBUG("Operation node updated (OperationId: %v)", operation->GetId());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation node %v",
                operation->GetId())
                << ex;
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

    void ExecuteCustomWatcherUpdate(const TWatcherRecord& watcher)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
        watcher.Requester.Run(batchReq);
        auto batchRspOrError = WaitFor(batchReq->Invoke());
        if (!batchRspOrError.IsOK()) {
            YT_LOG_WARNING(batchRspOrError, "Error updating custom watcher");
            return;
        }
        RunWatcherHandler(watcher, batchRspOrError.Value(), /* strictMode */ false);
    }

    void UpdateWatchers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(State_ == EMasterConnectorState::Connected);

        YT_LOG_DEBUG("Updating watchers");

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
        for (const auto& watcher : CommonWatcherRecords_) {
            watcher.Requester.Run(batchReq);
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
            RunWatcherHandler(watcher, batchRsp, /* strictMode */ false);
        }

        YT_LOG_DEBUG("Common watchers updated");
    }

    void RunWatcherHandler(const TWatcherRecord& watcher, TObjectServiceProxy::TRspExecuteBatchPtr responses, bool strictMode)
    {
        std::optional<ESchedulerAlertType> alertType = strictMode
            ? ESchedulerAlertType::SchedulerCannotConnect
            : watcher.AlertType;
        try {
            watcher.Handler.Run(responses);
            if (alertType) {
                SetSchedulerAlert(*alertType, TError());
            }
        } catch (TErrorException& ex) {
            if (ex.Error().GetCode() != EErrorCode::WatcherHandlerFailed) {
                throw;
            }
            if (alertType) {
                SetSchedulerAlert(*alertType, ex.Error());
            }
            if (strictMode) {
                UpdateAlerts();
                throw;
            }
            YT_LOG_WARNING(ex.Error());
        }
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

        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
        auto req = TYPathProxy::Set("//sys/scheduler/@alerts");
        req->set_value(ConvertToYsonString(alerts).ToString());

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
        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
        auto req = TYPathProxy::Set(FromObjectId(LockTransaction_->GetId()) + "/@timeout");
        req->set_value(ConvertToYsonString(timeout.MilliSeconds()).ToString());
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

void TMasterConnector::InvokeStoringStrategyState(TPersistentStrategyStatePtr strategyState)
{
    Impl_->InvokeStoringStrategyState(std::move(strategyState));
}

void TMasterConnector::StoreSchedulingSegmentsStateAsync(TPersistentSchedulingSegmentsStatePtr segmentsState)
{
    Impl_->InvokeStoringSchedulingSegmentsState(std::move(segmentsState));
}

void TMasterConnector::AttachJobContext(
    const TYPath& path,
    TChunkId chunkId,
    TOperationId operationId,
    TJobId jobId,
    const TString& user)
{
    return Impl_->AttachJobContext(path, chunkId, operationId, jobId, user);
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
    std::optional<ESchedulerAlertType> alertType)
{
    Impl_->SetCustomWatcher(type, std::move(requester), std::move(handler), period, alertType);
}

DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnecting, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterHandshake, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnected, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
