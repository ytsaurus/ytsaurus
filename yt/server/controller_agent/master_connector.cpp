#include "master_connector.h"
#include "operation_controller.h"
#include "snapshot_downloader.h"
#include "snapshot_builder.h"
#include "controller_agent.h"
#include "operation.h"
#include "serialize.h"
#include "helpers.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/helpers.h>

#include <yt/server/misc/update_executor.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>
#include <yt/client/api/transaction.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT::NControllerAgent {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NFileClient;
using namespace NSecurityClient;
using namespace NTransactionClient;
using namespace NScheduler;

using NYT::FromProto;
using NYT::ToProto;

using NNodeTrackerClient::GetDefaultAddress;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TControllerAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , InitialConfig_(Config_)
        , Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->SubscribeSchedulerConnecting(BIND(
            &TImpl::OnSchedulerConnecting,
            Unretained(this)));
        controllerAgent->SubscribeSchedulerConnected(BIND(
            &TImpl::OnSchedulerConnected,
            Unretained(this)));
        controllerAgent->SubscribeSchedulerDisconnected(BIND(
            &TImpl::OnSchedulerDisconnected,
            Unretained(this)));
    }

    void RegisterOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        OperationNodesUpdateExecutor_->AddUpdate(
            operationId,
            TOperationNodeUpdate(operationId));
    }

    void UnregisterOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        OperationNodesUpdateExecutor_->RemoveUpdate(operationId);
    }

    void CreateJobNode(TOperationId operationId, const TCreateJobNodeRequest& request)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        CancelableControlInvoker_->Invoke(BIND(
            &TImpl::DoCreateJobNode,
            MakeStrong(this),
            operationId,
            request));
    }

    TFuture<void> UpdateInitializedOperationNode(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoUpdateInitializedOperationNode, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    TFuture<void> FlushOperationNode(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        YT_LOG_INFO("Flushing operation node (OperationId: %v)",
            operationId);

        return OperationNodesUpdateExecutor_->ExecuteUpdate(operationId);
    }

    TFuture<void> AttachToLivePreview(
        TOperationId operationId,
        TTransactionId transactionId,
        TNodeId tableId,
        const std::vector<TChunkTreeId>& childIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        return BIND(&TImpl::DoAttachToLivePreview, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker_)
            .Run(operationId, transactionId, tableId, childIds);
    }

    TFuture<TOperationSnapshot> DownloadSnapshot(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        if (!Config_->EnableSnapshotLoading) {
            return MakeFuture<TOperationSnapshot>(TError("Snapshot loading is disabled in configuration"));
        }

        return BIND(&TImpl::DoDownloadSnapshot, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker_)
            .Run(operationId);
    }

    TFuture<void> RemoveSnapshot(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        return BIND(&TImpl::DoRemoveSnapshot, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    void AddChunkTreesToUnstageList(std::vector<TChunkTreeId> chunkTreeIds, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        CancelableControlInvoker_->Invoke(BIND(&TImpl::DoAddChunkTreesToUnstageList,
            MakeWeak(this),
            Passed(std::move(chunkTreeIds)),
            recursive));
    }

    TFuture<void> UpdateConfig()
    {
        return BIND(&TImpl::ExecuteUpdateConfig, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

private:
    TControllerAgentConfigPtr Config_;
    TControllerAgentConfigPtr InitialConfig_;

    TBootstrap* const Bootstrap_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableControlInvoker_;

    struct TLivePreviewRequest
    {
        TChunkListId TableId;
        TChunkTreeId ChildId;
    };

    struct TOperationNodeUpdate
    {
        explicit TOperationNodeUpdate(TOperationId operationId)
            : OperationId(operationId)
        { }

        TOperationId OperationId;

        std::vector<TCreateJobNodeRequest> JobRequests;

        TTransactionId LivePreviewTransactionId;
        std::vector<TLivePreviewRequest> LivePreviewRequests;
    };

    TIntrusivePtr<TUpdateExecutor<TOperationId, TOperationNodeUpdate>> OperationNodesUpdateExecutor_;

    TPeriodicExecutorPtr TransactionRefreshExecutor_;
    TPeriodicExecutorPtr SnapshotExecutor_;
    TPeriodicExecutorPtr UnstageExecutor_;

    TPeriodicExecutorPtr UpdateConfigExecutor_;
    TPeriodicExecutorPtr AlertsExecutor_;

    TEnumIndexedVector<TError, EControllerAgentAlertType> Alerts_;

    struct TUnstageRequest
    {
        TChunkTreeId ChunkTreeId;
        bool Recursive;
    };

    THashMap<TCellTag, std::vector<TUnstageRequest>> CellTagToUnstageList_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    bool IsConnected() const
    {
        return Bootstrap_->GetControllerAgent()->IsConnected();
    }

    void OnSchedulerConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        YCHECK(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();

        YCHECK(!CancelableControlInvoker_);
        CancelableControlInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());
    }

    void OnSchedulerConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(!OperationNodesUpdateExecutor_);
        OperationNodesUpdateExecutor_ = New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            CancelableControlInvoker_,
            BIND(&TImpl::UpdateOperationNode, Unretained(this)),
            BIND([] (const TOperationNodeUpdate*) { return false; }),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Config_->OperationsUpdatePeriod,
            Logger);
        OperationNodesUpdateExecutor_->Start();

        YCHECK(!TransactionRefreshExecutor_);
        TransactionRefreshExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::RefreshTransactions, MakeStrong(this)),
            Config_->TransactionsRefreshPeriod,
            EPeriodicExecutorMode::Automatic);
        TransactionRefreshExecutor_->Start();

        YCHECK(!SnapshotExecutor_);
        SnapshotExecutor_= New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::BuildSnapshot, MakeStrong(this)),
            Config_->SnapshotPeriod,
            EPeriodicExecutorMode::Automatic);
        SnapshotExecutor_->Start();

        YCHECK(!UnstageExecutor_);
        UnstageExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::UnstageChunkTrees, MakeWeak(this)),
            Config_->ChunkUnstagePeriod,
            EPeriodicExecutorMode::Automatic);
        UnstageExecutor_->Start();

        YCHECK(!UpdateConfigExecutor_);
        UpdateConfigExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::ExecuteUpdateConfig, MakeWeak(this)),
            Config_->ConfigUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        UpdateConfigExecutor_->Start();

        AlertsExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config_->AlertsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        AlertsExecutor_->Start();

        RegisterInstance();
    }

    void OnSchedulerDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();
    }

    void DoCleanup()
    {
        if (CancelableContext_) {
            CancelableContext_->Cancel();
            CancelableContext_.Reset();
        }

        CancelableControlInvoker_.Reset();

        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->Stop();
            OperationNodesUpdateExecutor_.Reset();
        }

        if (TransactionRefreshExecutor_) {
            TransactionRefreshExecutor_->Stop();
            TransactionRefreshExecutor_.Reset();
        }

        if (SnapshotExecutor_) {
            SnapshotExecutor_->Stop();
            SnapshotExecutor_.Reset();
        }

        if (UnstageExecutor_) {
            UnstageExecutor_->Stop();
            UnstageExecutor_.Reset();
        }

        if (UpdateConfigExecutor_) {
            UpdateConfigExecutor_->Stop();
            UpdateConfigExecutor_.Reset();
        }

        if (AlertsExecutor_) {
            AlertsExecutor_->Stop();
            AlertsExecutor_.Reset();
        }
    }

    TYPath GetInstancePath()
    {
        auto addresses = Bootstrap_->GetLocalAddresses();
        return "//sys/controller_agents/instances/" + ToYPathLiteral(GetDefaultAddress(addresses));
    }

    void RegisterInstance()
    {
        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
        auto batchReq = proxy.ExecuteBatch();
        auto path = GetInstancePath();
        {
            auto req = TCypressYPathProxy::Create(path);
            req->set_ignore_existing(true);
            req->set_recursive(true);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TCypressYPathProxy::Set(path + "/@annotations");
            req->set_value(ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations).GetData());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TCypressYPathProxy::Create(path + "/orchid");
            req->set_ignore_existing(true);
            req->set_recursive(true);
            req->set_type(static_cast<int>(EObjectType::Orchid));
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
            ToProto(req->mutable_node_attributes(), *attributes);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TYPathProxy::Set(path + "/@connection_time");
            req->set_value(ConvertToYsonString(TInstant::Now()).GetData());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(channelKind, cellTag));
        auto batchReq = proxy.ExecuteBatch();

        auto* prerequisitesExt = batchReq->Header().MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        ToProto(prerequisitesExt->add_transactions()->mutable_transaction_id(), Bootstrap_->GetControllerAgent()->GetIncarnationId());

        return batchReq;
    }

    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequestWithPrerequisites(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        auto batchReq = StartObjectBatchRequest(channelKind, cellTag);
        auto* prerequisitesExt = batchReq->Header().MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        ToProto(prerequisitesExt->add_transactions()->mutable_transaction_id(), Bootstrap_->GetControllerAgent()->GetIncarnationId());
        return batchReq;
    }

    TChunkServiceProxy::TReqExecuteBatchPtr StartChunkBatchRequest(TCellTag cellTag = PrimaryMasterCellTag)
    {
        TChunkServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag));
        return proxy.ExecuteBatch();
    }

    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Take a snapshot of all known operations.
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        // Collect all transactions that are used by currently running operations.
        THashSet<TTransactionId> watchSet;
        for (const auto& pair : controllerAgent->GetOperations()) {
            const auto& operation = pair.second;
            for (const auto& transactionId : operation->GetWatchTransactionIds()) {
                watchSet.insert(transactionId);
            }
        }

        THashMap<TCellTag, TObjectServiceProxy::TReqExecuteBatchPtr> batchReqs;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            if (batchReqs.find(cellTag) == batchReqs.end()) {
                auto connection = FindRemoteConnection(
                    Bootstrap_->GetMasterClient()->GetNativeConnection(),
                    cellTag);
                if (!connection) {
                    continue;
                }
                auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
                auto authenticatedChannel = CreateAuthenticatedChannel(channel, SchedulerUserName);
                TObjectServiceProxy proxy(authenticatedChannel);
                batchReqs[cellTag] = proxy.ExecuteBatch();
            }

            auto checkReq = TObjectYPathProxy::GetBasicAttributes(FromObjectId(id));
            batchReqs[cellTag]->AddRequest(checkReq, "check_tx_" + ToString(id));
        }

        YT_LOG_INFO("Refreshing transactions");

        THashMap<TCellTag, NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> batchRsps;

        for (const auto& pair : batchReqs) {
            auto cellTag = pair.first;
            const auto& batchReq = pair.second;
            auto batchRspOrError = WaitFor(batchReq->Invoke());
            if (batchRspOrError.IsOK()) {
                batchRsps[cellTag] = batchRspOrError.Value();
            } else {
                YT_LOG_ERROR(batchRspOrError, "Error refreshing transactions (CellTag: %v)",
                    cellTag);
            }
        }

        THashSet<TTransactionId> deadTransactionIds;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            auto it = batchRsps.find(cellTag);
            if (it != batchRsps.end()) {
                const auto& batchRsp = it->second;
                auto rspOrError = batchRsp->GetResponse("check_tx_" + ToString(id));
                if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(rspOrError, "Found dead transaction (TransactionId: %v)", id);
                    deadTransactionIds.insert(id);
                }
            }
        }

        YT_LOG_INFO("Transactions refreshed");

        // Check every transaction of every operation and raise appropriate notifications.
        for (const auto& pair : controllerAgent->GetOperations()) {
            const auto& operation = pair.second;
            auto controller = operation->GetController();
            std::vector<TTransactionId> locallyDeadTransactionIds;
            for (const auto& transactionId : operation->GetWatchTransactionIds()) {
                if (deadTransactionIds.find(transactionId) != deadTransactionIds.end()) {
                    locallyDeadTransactionIds.push_back(transactionId);
                }
            }
            if (!locallyDeadTransactionIds.empty()) {
                controller->GetCancelableInvoker()->Invoke(BIND(
                    &IOperationController::OnTransactionsAborted,
                    controller,
                    locallyDeadTransactionIds));
            }
        }
    }

    void DoUpdateInitializedOperationNode(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->GetOperation(operationId);

        auto batchReq = StartObjectBatchRequestWithPrerequisites();
        GenerateMutationId(batchReq);

        auto operationPath = GetOperationPath(operationId);

        // Update controller agent address.
        {
            auto req = TYPathProxy::Set(operationPath + "/@controller_agent_address");
            req->set_value(ConvertToYsonString(GetDefaultAddress(Bootstrap_->GetLocalAddresses())).GetData());
            batchReq->AddRequest(req, "set_controller_agent_address");
        }
        // Update controller agent orchid, it should point to this controller agent.
        {
            auto req = TCypressYPathProxy::Create(operationPath + "/controller_orchid");
            req->set_force(true);
            req->set_type(static_cast<int>(EObjectType::Orchid));
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
            attributes->Set("remote_root", "//controller_agent/operations/" + ToYPathLiteral(ToString(operationId)));
            ToProto(req->mutable_node_attributes(), *attributes);
            batchReq->AddRequest(req, "create_controller_orchid");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    void DoUpdateOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& operationId = operation->GetId();

        std::vector<TCreateJobNodeRequest> jobRequests;
        std::vector<TLivePreviewRequest> livePreviewRequests;
        TTransactionId livePreviewTransactionId;
        {
            auto* update = OperationNodesUpdateExecutor_->FindUpdate(operationId);
            if (!update) {
                return;
            }
            std::swap(jobRequests, update->JobRequests);
            std::swap(livePreviewRequests, update->LivePreviewRequests);
            livePreviewTransactionId = update->LivePreviewTransactionId;
        }

        YT_LOG_DEBUG("Started updating operation node (OperationId: %v, JobRequestCount: %v, "
            "LivePreviewTransactionId: %v, LivePreviewRequestCount: %v)",
            operationId,
            jobRequests.size(),
            livePreviewTransactionId,
            livePreviewRequests.size());

        std::vector<TCreateJobNodeRequest> successfulJobRequests;
        try {
            successfulJobRequests = CreateJobNodes(operation, jobRequests);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error creating job nodes (OperationId: %v)",
                operationId);
            auto error = TError("Error creating job nodes for operation %v",
                operationId)
                << ex;
            if (!error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded)) {
                THROW_ERROR error;
            }
        }

        try {
            std::vector<TJobFile> files;
            for (const auto& request : successfulJobRequests) {
                if (request.StderrChunkId) {
                    auto path = GetJobPath(
                        operationId,
                        request.JobId,
                        "stderr");

                    files.push_back({
                        request.JobId,
                        path,
                        request.StderrChunkId,
                        "stderr"
                    });
                }
                if (request.FailContextChunkId) {
                    auto path = GetJobPath(
                        operationId,
                        request.JobId,
                        "fail_context");

                    files.push_back({
                        request.JobId,
                        path,
                        request.FailContextChunkId,
                        "fail_context"
                    });
                }
            }
            SaveJobFiles(operationId, files);
        } catch (const std::exception& ex) {
            // NB: Don' treat this as a critical error.
            // Some of these chunks could go missing for a number of reasons.
            YT_LOG_WARNING(ex, "Error saving job files (OperationId: %v)",
                operationId);
        }

        try {
            AttachLivePreviewChunks(operationId, livePreviewTransactionId, livePreviewRequests);
        } catch (const std::exception& ex) {
            // NB: Don' treat this as a critical error.
            // Some of these chunks could go missing for a number of reasons.
            YT_LOG_WARNING(ex, "Error attaching live preview chunks (OperationId: %v)",
                operationId);
        }

        try {
            UpdateOperationNodes(operationId);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation %v node",
                operationId)
                << ex;
        }

        YT_LOG_DEBUG("Finished updating operation node (OperationId: %v)",
            operationId);
    }

    TCallback<TFuture<void>()> UpdateOperationNode(TOperationId operationId, TOperationNodeUpdate* update)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            return {};
        }

        auto controller = operation->GetController();

        if (update->JobRequests.empty() &&
            update->LivePreviewRequests.empty() &&
            !controller->ShouldUpdateProgress())
        {
            return {};
        }

        return BIND(&TImpl::DoUpdateOperationNode, MakeStrong(this), operation)
            .AsyncVia(CancelableControlInvoker_);
    }

    void UpdateOperationNodes(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            return;
        }

        auto controller = operation->GetController();
        if (!controller->HasProgress()) {
            return;
        }

        controller->SetProgressUpdated();

        auto batchReq = StartObjectBatchRequestWithPrerequisites();
        GenerateMutationId(batchReq);

        auto progress = controller->GetProgress();
        YCHECK(progress);

        auto briefProgress = controller->GetBriefProgress();
        YCHECK(briefProgress);

        auto operationPath = GetOperationPath(operationId);

        auto multisetReq = TYPathProxy::Multiset(operationPath + "/@");

        // Set progress.
        {
            auto req = multisetReq->add_subrequests();
            req->set_key("progress");
            req->set_value(progress.GetData());
        }

        // Set brief progress.
        {
            auto req = multisetReq->add_subrequests();
            req->set_key("brief_progress");
            req->set_value(briefProgress.GetData());
        }

        batchReq->AddRequest(multisetReq, "update_op_node");

        // This is needed to prevent controller lifetime prolongation due to strong pointer
        // being kept in the stack while waiting for a batch request being invoked.
        controller.Reset();

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    std::vector<TCreateJobNodeRequest> CreateJobNodes(
        const TOperationPtr& operation,
        const std::vector<TCreateJobNodeRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (requests.empty()) {
            return {};
        }

        auto batchReq = StartObjectBatchRequestWithPrerequisites();

        for (const auto& request : requests) {
            const auto& jobId = request.JobId;

            auto path = GetJobPath(operation->GetId(), jobId);
            auto attributes = ConvertToAttributes(request.Attributes);

            auto req = TCypressYPathProxy::Create(path);
            GenerateMutationId(req);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            req->set_force(true);
            ToProto(req->mutable_node_attributes(), *attributes);
            batchReq->AddRequest(req, "create_" + ToString(jobId));
        }

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        std::vector<TCreateJobNodeRequest> successfulRequests;
        for (const auto& request : requests) {
            const auto& jobId = request.JobId;
            auto rspsOrError = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_" + ToString(jobId));
            bool allOK = true;
            for (const auto& rspOrError : rspsOrError) {
                if (rspOrError.IsOK()) {
                    continue;
                }
                allOK = false;
                if (rspOrError.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded)) {
                    YT_LOG_ERROR(rspOrError, "Account limit exceeded while creating job node (JobId: %v)",
                        jobId);
                } else {
                    THROW_ERROR_EXCEPTION("Failed to create job node")
                        << TErrorAttribute("job_id", jobId)
                        << rspOrError;
                }
            }
            if (allOK) {
                successfulRequests.push_back(request);
            }
        }

        YT_LOG_INFO("Job nodes created (TotalCount: %v, SuccessCount: %v, OperationId: %v)",
            requests.size(),
            successfulRequests.size(),
            operation->GetId());

        return successfulRequests;
    }

    void AttachLivePreviewChunks(
        TOperationId operationId,
        TTransactionId transactionId,
        const std::vector<TLivePreviewRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        struct TTableInfo
        {
            TNodeId TableId;
            TCellTag CellTag;
            std::vector<TChunkId> ChildIds;
            TTransactionId UploadTransactionId;
            TChunkListId UploadChunkListId;
            NChunkClient::NProto::TDataStatistics Statistics;
        };

        THashMap<TNodeId, TTableInfo> tableIdToInfo;
        for (const auto& request : requests) {
            auto& tableInfo = tableIdToInfo[request.TableId];
            tableInfo.TableId = request.TableId;
            tableInfo.ChildIds.push_back(request.ChildId);

            YT_LOG_DEBUG("Appending live preview chunk trees (OperationId: %v, TableId: %v, ChildCount: %v)",
                operationId,
                tableInfo.TableId,
                tableInfo.ChildIds.size());
        }

        if (tableIdToInfo.empty()) {
            return;
        }

        // BeginUpload
        {
            auto batchReq = StartObjectBatchRequestWithPrerequisites();

            for (const auto& pair : tableIdToInfo) {
                const auto& tableId = pair.first;
                {
                    auto req = TTableYPathProxy::BeginUpload(FromObjectId(tableId));
                    req->set_update_mode(static_cast<int>(EUpdateMode::Append));
                    req->set_lock_mode(static_cast<int>(ELockMode::Shared));
                    req->set_upload_transaction_title(Format("Attaching live preview chunks of operation %v",
                        operationId));
                    SetTransactionId(req, transactionId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "begin_upload");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto rsps = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspBeginUpload>("begin_upload");
            int rspIndex = 0;
            for (auto& pair : tableIdToInfo) {
                auto& tableInfo = pair.second;
                const auto& rsp = rsps[rspIndex++].Value();
                tableInfo.CellTag = rsp->cell_tag();
                tableInfo.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            }
        }

        THashMap<TCellTag, std::vector<TTableInfo*>> cellTagToInfos;
        for (auto& pair : tableIdToInfo) {
            auto& tableInfo  = pair.second;
            cellTagToInfos[tableInfo.CellTag].push_back(&tableInfo);
        }

        // GetUploadParams
        for (auto& pair : cellTagToInfos) {
            auto cellTag = pair.first;
            auto& tableInfos = pair.second;

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower, cellTag);
            for (const auto* tableInfo : tableInfos) {
                auto req = TTableYPathProxy::GetUploadParams(FromObjectId(tableInfo->TableId));
                SetTransactionId(req, tableInfo->UploadTransactionId);
                batchReq->AddRequest(req, "get_upload_params");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto rsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetUploadParams>("get_upload_params");
            int rspIndex = 0;
            for (auto* tableInfo : tableInfos) {
                const auto& rsp = rsps[rspIndex++].Value();
                tableInfo->UploadChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            }
        }

        // Attach
        for (auto& pair : cellTagToInfos) {
            auto cellTag = pair.first;
            auto& tableInfos = pair.second;

            auto batchReq = StartChunkBatchRequest(cellTag);
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            std::vector<int> tableIndexToRspIndex;
            for (const auto* tableInfo : tableInfos) {
                size_t beginIndex = 0;
                const auto& childIds = tableInfo->ChildIds;
                while (beginIndex < childIds.size()) {
                    auto lastIndex = std::min(beginIndex + Config_->MaxChildrenPerAttachRequest, childIds.size());
                    bool isFinal = (lastIndex == childIds.size());
                    if (isFinal) {
                        tableIndexToRspIndex.push_back(batchReq->attach_chunk_trees_subrequests_size());
                    }
                    auto* req = batchReq->add_attach_chunk_trees_subrequests();
                    ToProto(req->mutable_parent_id(), tableInfo->UploadChunkListId);
                    for (auto index = beginIndex; index < lastIndex; ++index) {
                        ToProto(req->add_child_ids(), childIds[index]);
                    }
                    req->set_request_statistics(isFinal);
                    beginIndex = lastIndex;
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            const auto& rsps = batchRsp->attach_chunk_trees_subresponses();
            for (int tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
                auto* tableInfo = tableInfos[tableIndex];
                const auto& rsp = rsps.Get(tableIndexToRspIndex[tableIndex]);
                tableInfo->Statistics = rsp.statistics();
            }
        }

        // EndUpload
        {
            auto batchReq = StartObjectBatchRequestWithPrerequisites();

            for (const auto& pair : tableIdToInfo) {
                const auto& tableId = pair.first;
                const auto& tableInfo = pair.second;
                {
                    auto req = TTableYPathProxy::EndUpload(FromObjectId(tableId));
                    *req->mutable_statistics() = tableInfo.Statistics;
                    SetTransactionId(req, tableInfo.UploadTransactionId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "end_upload");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }
    }

    void DoAttachToLivePreview(
        TOperationId operationId,
        TTransactionId transactionId,
        TNodeId tableId,
        const std::vector<TChunkTreeId>& childIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* update = OperationNodesUpdateExecutor_->FindUpdate(operationId);
        if (!update) {
            YT_LOG_DEBUG("Trying to attach live preview to an unknown operation (OperationId: %v)",
                operationId);
            return;
        }

        // NB: Controller must attach all live preview chunks under the same transaction.
        YCHECK(!update->LivePreviewTransactionId || update->LivePreviewTransactionId == transactionId);
        update->LivePreviewTransactionId = transactionId;

        YT_LOG_TRACE("Attaching live preview chunk trees (OperationId: %v, TableId: %v, ChildCount: %v)",
            operationId,
            tableId,
            childIds.size());

        for (const auto& childId : childIds) {
            update->LivePreviewRequests.push_back(TLivePreviewRequest{tableId, childId});
        }
    }

    TOperationSnapshot DoDownloadSnapshot(TOperationId operationId)
    {
        auto batchReq = StartObjectBatchRequest();

        {
            auto req = TYPathProxy::Get(GetSnapshotPath(operationId) + "/@version");
            batchReq->AddRequest(req, "get_version");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        const auto& batchRsp = batchRspOrError.ValueOrThrow();

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_version");


        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION("Snapshot does not exist");
            }

            THROW_ERROR_EXCEPTION("Error getting snapshot version")
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        int version = ConvertTo<int>(TYsonString(rsp->value()));

        YT_LOG_INFO("Snapshot found (OperationId: %v, Version: %v)",
            operationId,
            version);

        if (!ValidateSnapshotVersion(version)) {
            THROW_ERROR_EXCEPTION("Snapshot version validation failed");
        }

        TOperationSnapshot snapshot;
        snapshot.Version = version;
        try {
            auto downloader = New<TSnapshotDownloader>(
                Config_,
                Bootstrap_,
                operationId);
            snapshot.Blocks = downloader->Run();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error downloading snapshot") << ex;
        }
        return snapshot;
    }

    void DoCreateJobNode(TOperationId operationId, const TCreateJobNodeRequest& request)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* update = OperationNodesUpdateExecutor_->FindUpdate(operationId);
        if (!update) {
            YT_LOG_DEBUG("Requested to create a job node for an unknown operation (OperationId: %v, JobId: %v)",
                operationId,
                request.JobId);
            return;
        }

        YT_LOG_DEBUG("Job node creation scheduled (OperationId: %v, JobId: %v, StderrChunkId: %v, FailContextChunkId: %v)",
            operationId,
            request.JobId,
            request.StderrChunkId,
            request.FailContextChunkId);

        update->JobRequests.emplace_back(request);
    }

    void DoRemoveSnapshot(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto batchReq = StartObjectBatchRequestWithPrerequisites();
        {
            auto req = TYPathProxy::Remove(GetSnapshotPath(operationId));
            req->set_force(true);
            batchReq->AddRequest(req, "remove_snapshot");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        auto error = GetCumulativeError(batchRspOrError);
        if (!error.IsOK()) {
            Bootstrap_->GetControllerAgent()->Disconnect(TError("Failed to remove snapshot") << error);
        }
    }

    void SaveJobFiles(TOperationId operationId, const std::vector<TJobFile>& files)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NScheduler::SaveJobFiles(Bootstrap_->GetMasterClient(), operationId, files);
    }

    void BuildSnapshot()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Config_->EnableSnapshotBuilding) {
            return;
        }

        TOperationIdToWeakControllerMap weakControllerMap;

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto controllerMap = controllerAgent->GetOperations();
        for (const auto& pair : controllerMap) {
            weakControllerMap.insert({pair.first, pair.second->GetController()});
        }

        auto builder = New<TSnapshotBuilder>(
            Config_,
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetControllerAgent()->GetSnapshotIOInvoker(),
            Bootstrap_->GetControllerAgent()->GetIncarnationId());

        // NB: Result is logged in the builder.
        auto error = WaitFor(builder->Run(weakControllerMap));
        if (error.IsOK()) {
            YT_LOG_INFO("Snapshot builder finished");
        } else {
            YT_LOG_ERROR(error, "Error building snapshots");
        }
    }

    bool IsOperationInFinishedState(const TOperationNodeUpdate* update) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return !Bootstrap_->GetControllerAgent()->FindOperation(update->OperationId);
    }

    void OnOperationUpdateFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Bootstrap_->GetControllerAgent()->Disconnect(TError("Failed to update operation node") << error);
    }

    void DoAddChunkTreesToUnstageList(std::vector<TChunkTreeId> chunkTreeIds, bool recursive)
    {
        for (const auto& chunkTreeId : chunkTreeIds) {
            auto cellTag = CellTagFromId(chunkTreeId);
            CellTagToUnstageList_[cellTag].emplace_back(TUnstageRequest{chunkTreeId, recursive});
        }
    }

    void UnstageChunkTrees()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto& pair : CellTagToUnstageList_) {
            const auto& cellTag = pair.first;
            auto& unstageRequests = pair.second;

            if (unstageRequests.empty()) {
                continue;
            }

            TChunkServiceProxy proxy(Bootstrap_
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Leader, cellTag));

            auto batchReq = proxy.ExecuteBatch();
            while (!unstageRequests.empty() &&
                batchReq->unstage_chunk_tree_subrequests_size() < Config_->DesiredChunkListsPerRelease)
            {
                auto unstageRequest = unstageRequests.back();
                unstageRequests.pop_back();
                auto req = batchReq->add_unstage_chunk_tree_subrequests();
                ToProto(req->mutable_chunk_tree_id(), unstageRequest.ChunkTreeId);
                req->set_recursive(unstageRequest.Recursive);
            }

            YT_LOG_DEBUG("Unstaging chunk trees (ChunkTreeCount: %v, CellTag: %v)",
                batchReq->unstage_chunk_tree_subrequests_size(),
                cellTag);

            batchReq->Invoke().Apply(
                BIND([=] (const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                    if (!batchRspOrError.IsOK()) {
                        YT_LOG_DEBUG(batchRspOrError, "Error unstaging chunk trees (CellTag: %v)", cellTag);
                    }
                }));
        }
    }

    void DoUpdateConfig(TControllerAgentConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config_ = config;

        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->SetPeriod(Config_->OperationsUpdatePeriod);
        }
        if (TransactionRefreshExecutor_) {
            TransactionRefreshExecutor_->SetPeriod(Config_->TransactionsRefreshPeriod);
        }
        if (SnapshotExecutor_) {
            SnapshotExecutor_->SetPeriod(Config_->SnapshotPeriod);
        }
        if (UnstageExecutor_) {
            UnstageExecutor_->SetPeriod(Config_->ChunkUnstagePeriod);
        }
        if (UpdateConfigExecutor_) {
            UpdateConfigExecutor_->SetPeriod(Config_->ConfigUpdatePeriod);
        }
        if (AlertsExecutor_) {
            AlertsExecutor_->SetPeriod(Config_->AlertsUpdatePeriod);
        }
    }



    void ValidateConfig()
    {
        // First reset the alerts.
        SetControllerAgentAlert(EControllerAgentAlertType::UnrecognizedConfigOptions, TError());
        SetControllerAgentAlert(EControllerAgentAlertType::SnapshotLoadingDisabled, TError());

        if (Config_->EnableUnrecognizedAlert) {
            auto unrecognized = Config_->GetUnrecognizedRecursively();
            if (unrecognized && unrecognized->GetChildCount() > 0) {
                YT_LOG_WARNING("Controller agent config contains unrecognized options (Unrecognized: %v)",
                    ConvertToYsonString(unrecognized, EYsonFormat::Text));
                SetControllerAgentAlert(
                    EControllerAgentAlertType::UnrecognizedConfigOptions,
                    TError("Controller agent config contains unrecognized options")
                        << TErrorAttribute("unrecognized", unrecognized));
            }
        }

        if (!Config_->EnableSnapshotLoading) {
            auto error = TError("Snapshot loading is disabled; consider enabling it using the controller agent config");
            YT_LOG_WARNING(error);
            SetControllerAgentAlert(EControllerAgentAlertType::SnapshotLoadingDisabled, error);
        }
    }

    void ExecuteUpdateConfig()
    {
        YT_LOG_INFO("Updating controller agent configuration");

        try {
            TObjectServiceProxy proxy(Bootstrap_
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Follower));

            auto req = TYPathProxy::Get("//sys/controller_agents/config");
            auto rspOrError = WaitFor(proxy.Execute(req));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_INFO("No configuration found in Cypress");
                SetControllerAgentAlert(EControllerAgentAlertType::UpdateConfig, TError());
                return;
            }

            TControllerAgentConfigPtr newConfig;
            try {
                const auto& rsp = rspOrError.ValueOrThrow();

                newConfig = CloneYsonSerializable(InitialConfig_);
                newConfig->Load(ConvertToNode(TYsonString(rsp->value())), /* validate */ true, /* setDefaults */ false);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error loading controller agent configuration")
                    << ex;
            }

            SetControllerAgentAlert(EControllerAgentAlertType::UpdateConfig, TError());

            auto oldConfigNode = ConvertToNode(Config_);
            auto newConfigNode = ConvertToNode(newConfig);
            if (AreNodesEqual(oldConfigNode, newConfigNode)) {
                return;
            }

            DoUpdateConfig(newConfig);
            ValidateConfig();

            Bootstrap_->GetControllerAgent()->UpdateConfig(newConfig);

            YT_LOG_INFO("Controller agent configuration updated");
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            SetControllerAgentAlert(EControllerAgentAlertType::UpdateConfig, error);
            YT_LOG_WARNING(error, "Error updating controller agent configuration");
        }
    }

    void SetControllerAgentAlert(EControllerAgentAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts_[alertType] = alert;
    }

    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IsConnected());

        std::vector<TError> alerts;
        for (auto alertType : TEnumTraits<EControllerAgentAlertType>::GetDomainValues()) {
            const auto& alert = Alerts_[alertType];
            if (!alert.IsOK()) {
                alerts.push_back(alert);
            }
        }

        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
        auto req = TYPathProxy::Set(GetInstancePath() + "/@alerts");
        req->set_value(ConvertToYsonString(alerts).GetData());
        req->set_recursive(true);

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error updating controller agent alerts");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TControllerAgentConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TMasterConnector::~TMasterConnector() = default;

void TMasterConnector::Initialize()
{
    Impl_->Initialize();
}

void TMasterConnector::RegisterOperation(TOperationId operationId)
{
    Impl_->RegisterOperation(operationId);
}

void TMasterConnector::UnregisterOperation(TOperationId operationId)
{
    Impl_->UnregisterOperation(operationId);
}

void TMasterConnector::CreateJobNode(TOperationId operationId, const TCreateJobNodeRequest& request)
{
    Impl_->CreateJobNode(operationId, request);
}

TFuture<void> TMasterConnector::FlushOperationNode(TOperationId operationId)
{
    return Impl_->FlushOperationNode(operationId);
}

TFuture<void> TMasterConnector::UpdateInitializedOperationNode(TOperationId operationId)
{
    return Impl_->UpdateInitializedOperationNode(operationId);
}

TFuture<void> TMasterConnector::AttachToLivePreview(
    TOperationId operationId,
    TTransactionId transactionId,
    TNodeId tableId,
    const std::vector<TChunkTreeId>& childIds)
{
    return Impl_->AttachToLivePreview(operationId, transactionId, tableId, childIds);
}

TFuture<TOperationSnapshot> TMasterConnector::DownloadSnapshot(TOperationId operationId)
{
    return Impl_->DownloadSnapshot(operationId);
}

TFuture<void> TMasterConnector::RemoveSnapshot(TOperationId operationId)
{
    return Impl_->RemoveSnapshot(operationId);
}

void TMasterConnector::AddChunkTreesToUnstageList(std::vector<TChunkId> chunkTreeIds, bool recursive)
{
    Impl_->AddChunkTreesToUnstageList(std::move(chunkTreeIds), recursive);
}

TFuture<void> TMasterConnector::UpdateConfig()
{
    return Impl_->UpdateConfig();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent


