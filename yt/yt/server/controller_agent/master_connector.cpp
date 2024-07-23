#include "master_connector.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "helpers.h"
#include "operation.h"
#include "operation_controller.h"
#include "private.h"
#include "snapshot_builder.h"
#include "snapshot_downloader.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/misc/update_executor.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/operations_archive_schema.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <library/cpp/iterator/zip.h>

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

static constexpr auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TControllerAgentConfigPtr config,
        INodePtr configNode,
        TBootstrap* bootstrap)
        : Config_(std::move(config))
        , InitialConfigNode_(std::move(configNode))
        , Bootstrap_(bootstrap)
        , ForkCounters_(New<TForkCounters>(ControllerAgentProfiler))
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
        YT_VERIFY(IsConnected());

        OperationNodesAndArchiveUpdateExecutor_->AddUpdate(
            operationId,
            TOperationNodeUpdate(operationId));
    }

    void UnregisterOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

        OperationNodesAndArchiveUpdateExecutor_->RemoveUpdate(operationId);
    }

    TFuture<void> UpdateInitializedOperationNode(TOperationId operationId, bool isCleanOperationStart)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoUpdateInitializedOperationNode, MakeStrong(this), operationId, isCleanOperationStart)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    TFuture<void> UpdateControllerFeatures(TOperationId operationId, const TYsonString& featureYson)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoUpdateControllerFeatures, MakeStrong(this), operationId, featureYson)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    TFuture<void> FlushOperationNode(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

        YT_LOG_INFO("Flushing operation node (OperationId: %v)",
            operationId);

        return OperationNodesAndArchiveUpdateExecutor_->ExecuteUpdate(operationId);
    }

    TFuture<void> AttachToLivePreview(
        TOperationId operationId,
        TTransactionId transactionId,
        TNodeId tableId,
        const std::vector<TChunkTreeId>& childIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

        return BIND(&TImpl::DoAttachToLivePreview, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker_)
            .Run(operationId, transactionId, tableId, childIds);
    }

    TFuture<void> UpdateAccountResourceUsageLease(
        NSecurityClient::TAccountResourceUsageLeaseId leaseId,
        const TDiskQuota& diskQuota)
    {
        return BIND(&TImpl::DoUpdateAccountResourceUsageLease, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker_)
            .Run(leaseId, diskQuota);
    }

    TFuture<TOperationSnapshot> DownloadSnapshot(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

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
        YT_VERIFY(IsConnected());

        return BIND(&TImpl::DoRemoveSnapshot, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    void AddChunkTreesToUnstageList(std::vector<TChunkTreeId> chunkTreeIds, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

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

    ui64 GetConfigRevision() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        return ConfigRevision_;
    }

    bool TagsLoaded() const
    {
        return static_cast<bool>(Tags_);
    }

    const std::vector<TString>& GetTags() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Tags_) {
            return *Tags_;
        }

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());

        YT_LOG_DEBUG("Fetching \"tags_override\" attribute");

        auto req = TYPathProxy::Get(GetInstancePath() + "/@tags_override");
        auto rspOrError = WaitFor(proxy.Execute(req));
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            Tags_ = Config_->Tags;
            YT_LOG_DEBUG("Attribute \"tags_override\" does not exist; using tags from config (Tags: %v)",
                Tags_);
        } else {
            auto rsp = rspOrError.ValueOrThrow();
            Tags_ = ConvertTo<std::vector<TString>>(TYsonString(rsp->value()));
            YT_LOG_DEBUG("Tags fetched from Cypress (Tags: %v)",
                Tags_);
        }

        return *Tags_;
    }

    void SetControllerAgentAlert(EControllerAgentAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts_[alertType] = alert;
    }

private:
    TControllerAgentConfigPtr Config_;
    INodePtr InitialConfigNode_;
    ui64 ConfigRevision_ = 0;

    TBootstrap* const Bootstrap_;

    std::optional<bool> ArchiveExists_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableControlInvoker_;

    mutable std::optional<std::vector<TString>> Tags_;

    struct TLivePreviewRequest
    {
        TNodeId TableId;
        TChunkTreeId ChildId;
    };

    struct TOperationNodeUpdate
    {
        explicit TOperationNodeUpdate(TOperationId operationId)
            : OperationId(operationId)
        { }

        TOperationId OperationId;

        TTransactionId LivePreviewTransactionId;
        std::vector<TLivePreviewRequest> LivePreviewRequests;
    };

    TIntrusivePtr<TUpdateExecutor<TOperationId, TOperationNodeUpdate>> OperationNodesAndArchiveUpdateExecutor_;

    TPeriodicExecutorPtr TransactionRefreshExecutor_;
    TPeriodicExecutorPtr SnapshotExecutor_;
    TPeriodicExecutorPtr UnstageExecutor_;

    TPeriodicExecutorPtr UpdateConfigExecutor_;
    TPeriodicExecutorPtr AlertsExecutor_;
    TPeriodicExecutorPtr UpdateIntermediateMediumUsageExecutor_;

    TEnumIndexedArray<EControllerAgentAlertType, TError> Alerts_;

    NProfiling::TCounter UpdateOperationProgressFailuresCounter_ = ControllerAgentProfiler
        .Counter("/operations_archive/update_progress_failures");

    TForkCountersPtr ForkCounters_;

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

    bool DoesOperationsArchiveExist()
    {
        if (!ArchiveExists_) {
            ArchiveExists_ = Bootstrap_->GetClient()->DoesOperationsArchiveExist();
        }
        return *ArchiveExists_;
    }

    void OnSchedulerConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        YT_VERIFY(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();

        YT_VERIFY(!CancelableControlInvoker_);
        CancelableControlInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());
    }

    void OnSchedulerConnected(TIncarnationId incarnationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!OperationNodesAndArchiveUpdateExecutor_);
        OperationNodesAndArchiveUpdateExecutor_ = New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            CancelableControlInvoker_,
            BIND(&TImpl::UpdateOperationNodeAndArchive, Unretained(this)),
            BIND([] (const TOperationNodeUpdate*) { return false; }),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Config_->OperationsUpdatePeriod,
            Logger());
        OperationNodesAndArchiveUpdateExecutor_->Start();

        YT_VERIFY(!TransactionRefreshExecutor_);
        TransactionRefreshExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::RefreshTransactions, MakeStrong(this)),
            Config_->TransactionsRefreshPeriod);
        TransactionRefreshExecutor_->Start();

        YT_VERIFY(!SnapshotExecutor_);
        SnapshotExecutor_= New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::BuildSnapshot, MakeStrong(this)),
            Config_->SnapshotPeriod);
        SnapshotExecutor_->Start();

        YT_VERIFY(!UnstageExecutor_);
        UnstageExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::UnstageChunkTrees, MakeWeak(this)),
            Config_->ChunkUnstagePeriod);
        UnstageExecutor_->Start();

        YT_VERIFY(!UpdateConfigExecutor_);
        UpdateConfigExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::ExecuteUpdateConfig, MakeWeak(this)),
            Config_->ConfigUpdatePeriod);
        UpdateConfigExecutor_->Start();

        YT_VERIFY(!AlertsExecutor_);
        AlertsExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config_->AlertsUpdatePeriod);
        AlertsExecutor_->Start();

        YT_VERIFY(!UpdateIntermediateMediumUsageExecutor_);
        UpdateIntermediateMediumUsageExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TImpl::UpdateIntermediateMediumUsage, MakeStrong(this)),
            Config_->IntermediateMediumUsageUpdatePeriod);
        UpdateIntermediateMediumUsageExecutor_->Start();

        RegisterInstance(incarnationId);
    }

    void OnSchedulerDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();
    }

    void DoCleanup()
    {
        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("Scheduler disconnected"));
            CancelableContext_.Reset();
        }

        CancelableControlInvoker_.Reset();

        if (OperationNodesAndArchiveUpdateExecutor_) {
            OperationNodesAndArchiveUpdateExecutor_->Stop();
            OperationNodesAndArchiveUpdateExecutor_.Reset();
        }

        if (TransactionRefreshExecutor_) {
            YT_UNUSED_FUTURE(TransactionRefreshExecutor_->Stop());
            TransactionRefreshExecutor_.Reset();
        }

        if (SnapshotExecutor_) {
            YT_UNUSED_FUTURE(SnapshotExecutor_->Stop());
            SnapshotExecutor_.Reset();
        }

        if (UnstageExecutor_) {
            YT_UNUSED_FUTURE(UnstageExecutor_->Stop());
            UnstageExecutor_.Reset();
        }

        if (UpdateConfigExecutor_) {
            YT_UNUSED_FUTURE(UpdateConfigExecutor_->Stop());
            UpdateConfigExecutor_.Reset();
        }

        if (AlertsExecutor_) {
            YT_UNUSED_FUTURE(AlertsExecutor_->Stop());
            AlertsExecutor_.Reset();
        }

        if (UpdateIntermediateMediumUsageExecutor_) {
            YT_UNUSED_FUTURE(UpdateIntermediateMediumUsageExecutor_->Stop());
            UpdateIntermediateMediumUsageExecutor_.Reset();
        }
    }

    TYPath GetInstancePath() const
    {
        auto addresses = Bootstrap_->GetLocalAddresses();
        return "//sys/controller_agents/instances/" + ToYPathLiteral(GetDefaultAddress(addresses));
    }

    void RegisterInstance(TIncarnationId incarnationId)
    {
        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto batchReq = proxy.ExecuteBatch();
        auto path = GetInstancePath();
        {
            auto req = TCypressYPathProxy::Create(path + "/lock");
            req->set_ignore_existing(true);
            req->set_recursive(true);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TCypressYPathProxy::Set(path + "/@annotations");
            req->set_value(ConvertToYsonStringNestingLimited(Bootstrap_->GetConfig()->CypressAnnotations).ToString());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TCypressYPathProxy::Create(path + "/orchid");
            req->set_ignore_existing(true);
            req->set_recursive(true);
            req->set_type(static_cast<int>(EObjectType::Orchid));
            auto attributes = CreateEphemeralAttributesNestingLimited();
            attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
            ToProto(req->mutable_node_attributes(), *attributes);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TYPathProxy::Set(path + "/@connection_time");
            req->set_value(ConvertToYsonStringNestingLimited(TInstant::Now()).ToString());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }
        {
            auto req = TYPathProxy::Set(path + "/@tags");
            req->set_value(ConvertToYsonStringNestingLimited(GetTags()).ToString());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

        auto incarnationTransaction = Bootstrap_->GetClient()->AttachTransaction(
            NTransactionClient::TTransactionId{incarnationId});

        WaitFor(incarnationTransaction->LockNode(path + "/lock", NCypressClient::ELockMode::Exclusive))
            .ThrowOnError();
    }

    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTagSentinel)
    {
        TObjectServiceProxy proxy(
            Bootstrap_->GetClient(),
            channelKind,
            cellTag,
            /*stickyGroupSizeCache*/ nullptr);
        return proxy.ExecuteBatch();
    }

    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequestWithPrerequisites(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTagSentinel)
    {
        auto batchReq = StartObjectBatchRequest(channelKind, cellTag);
        auto* prerequisitesExt = batchReq->Header().MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        ToProto(prerequisitesExt->add_transactions()->mutable_transaction_id(), Bootstrap_->GetControllerAgent()->GetIncarnationId());
        return batchReq;
    }

    TChunkServiceProxy::TReqExecuteBatchPtr StartChunkBatchRequest(TCellTag cellTag = PrimaryMasterCellTagSentinel)
    {
        TChunkServiceProxy proxy(Bootstrap_
            ->GetClient()
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
        for (const auto& [operationId, operation] : controllerAgent->GetOperations()) {
            for (auto transactionId : operation->GetWatchTransactionIds()) {
                watchSet.insert(transactionId);
            }
        }

        THashMap<TCellTag, TObjectServiceProxy::TReqExecuteBatchPtr> batchReqs;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            if (batchReqs.find(cellTag) == batchReqs.end()) {
                auto connection = FindRemoteConnection(
                    Bootstrap_->GetClient()->GetNativeConnection(),
                    cellTag);
                if (!connection) {
                    continue;
                }
                auto proxy = CreateObjectServiceReadProxy(Bootstrap_->GetClient(), EMasterChannelKind::Follower);
                batchReqs[cellTag] = proxy.ExecuteBatch();
            }

            auto checkReq = TObjectYPathProxy::GetBasicAttributes(FromObjectId(id));
            batchReqs[cellTag]->AddRequest(checkReq, "check_tx_" + ToString(id));
        }

        YT_LOG_INFO("Refreshing transactions");

        THashMap<TCellTag, NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> batchRsps;

        for (const auto& [cellTag, batchReq] : batchReqs) {
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
        for (const auto& [operationId, operation] : controllerAgent->GetOperations()) {
            auto controller = operation->GetController();
            std::vector<TTransactionId> locallyDeadTransactionIds;
            for (auto transactionId : operation->GetWatchTransactionIds()) {
                if (deadTransactionIds.contains(transactionId)) {
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

    void DoUpdateInitializedOperationNode(TOperationId operationId, bool isCleanOperationStart)
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
            req->set_value(ConvertToYsonStringNestingLimited(GetDefaultAddress(Bootstrap_->GetLocalAddresses())).ToString());
            batchReq->AddRequest(req, "set_controller_agent_address");
        }
        // Initialize info about operation's failed jobs.
        if (isCleanOperationStart) {
            auto req = TYPathProxy::Set(operationPath + "/@has_failed_jobs");
            req->set_value(ConvertToYsonStringNestingLimited(false).ToString());
            batchReq->AddRequest(req, "set_has_failed_jobs");
        }
        // Update controller agent orchid, it should point to this controller agent.
        {
            auto req = TCypressYPathProxy::Create(operationPath + "/controller_orchid");
            req->set_force(true);
            req->set_type(static_cast<int>(EObjectType::Orchid));
            auto attributes = CreateEphemeralAttributesNestingLimited();
            attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
            attributes->Set("remote_root", "//controller_agent/operations/" + ToYPathLiteral(ToString(operationId)));
            ToProto(req->mutable_node_attributes(), *attributes);
            batchReq->AddRequest(req, "create_controller_orchid");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    void DoUpdateOperationNodeAndArchive(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();

        std::vector<TLivePreviewRequest> livePreviewRequests;
        TTransactionId livePreviewTransactionId;
        {
            auto* update = OperationNodesAndArchiveUpdateExecutor_->FindUpdate(operationId);
            if (!update) {
                return;
            }

            std::swap(livePreviewRequests, update->LivePreviewRequests);
            livePreviewTransactionId = update->LivePreviewTransactionId;
        }

        YT_LOG_DEBUG("Started updating operation node (OperationId: %v, "
            "LivePreviewTransactionId: %v, LivePreviewRequestCount: %v)",
            operationId,
            livePreviewTransactionId,
            livePreviewRequests.size());

        try {
            AttachLivePreviewChunks(operationId, livePreviewTransactionId, livePreviewRequests);
        } catch (const std::exception& ex) {
            // NB: Don' treat this as a critical error.
            // Some of these chunks could go missing for a number of reasons.
            YT_LOG_WARNING(ex, "Error attaching live preview chunks (OperationId: %v)",
                operationId);
        }

        try {
            UpdateOperationProgress(operationId);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation %v node",
                operationId)
                << ex;
        }

        YT_LOG_DEBUG("Finished updating operation node (OperationId: %v)",
            operationId);
    }

    TCallback<TFuture<void>()> UpdateOperationNodeAndArchive(TOperationId operationId, TOperationNodeUpdate* update)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            return {};
        }

        auto controller = operation->GetController();

        if (update->LivePreviewRequests.empty() &&
            !controller->ShouldUpdateProgressAttributes())
        {
            return {};
        }

        return BIND(&TImpl::DoUpdateOperationNodeAndArchive, MakeStrong(this), operation)
            .AsyncVia(CancelableControlInvoker_);
    }

    void DoUpdateControllerFeatures(TOperationId operationId, const TYsonString& featureYson)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            return;
        }

        auto controller = operation->GetController();

        if (!Config_->EnableControllerFeaturesArchivation ||
            !DoesOperationsArchiveExist() ||
            !TryUpdateControllerFeaturesInArchive(operationId, featureYson))
        {
            auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());

            auto path = GetOperationPath(operationId) + "/@controller_features";
            auto req = TYPathProxy::Set(path);
            req->set_value(featureYson.ToString());

            WaitFor(proxy.Execute(req))
                .ThrowOnError();
        }
    }

    bool TryUpdateControllerFeaturesInArchive(TOperationId operationId, const TYsonString& featureYson)
    {
        const auto& client = Bootstrap_->GetClient();
        auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, TTransactionStartOptions{}))
            .ValueOrThrow();
        YT_LOG_DEBUG("Operation controller features update transaction started (TransactionId: %v, OperationId: %v)",
            transaction->GetId(),
            operationId);

        auto featureYsonString = featureYson.ToString();

        TOrderedByIdTableDescriptor tableDescriptor;
        TUnversionedRowBuilder builder;
        auto operationIdAsGuid = operationId.Underlying();
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[0], tableDescriptor.Index.IdHi));
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[1], tableDescriptor.Index.IdLo));
        builder.AddValue(MakeUnversionedAnyValue(featureYsonString, tableDescriptor.Index.ControllerFeatures));

        auto rowBuffer = New<TRowBuffer>();
        auto row = rowBuffer->CaptureRow(builder.GetRow());
        i64 orderedByIdRowsDataWeight = GetDataWeight(row);

        transaction->WriteRows(
            GetOperationsArchiveOrderedByIdPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(TCompactVector<TUnversionedRow, 1>{1, row}, std::move(rowBuffer)));

        auto error = WaitFor(transaction->Commit()
            .ToUncancelable());

        if (!error.IsOK()) {
            YT_LOG_WARNING(
                error,
                "Operation controller features update in archive failed (TransactionId: %v, OperationId: %v)",
                transaction->GetId(),
                operationId);
        } else {
            YT_LOG_DEBUG("Operation controller features updated successfully (TransactionId: %v, DataWeight: %v, OperationId: %v)",
                transaction->GetId(),
                orderedByIdRowsDataWeight,
                operationId);
        }
        return error.IsOK();
    }

    void UpdateOperationProgress(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            return;
        }

        auto controller = operation->GetController();
        bool shouldUpdateLightOperationAttributes = controller->ShouldUpdateLightOperationAttributes();

        UpdateProgressAndLightAttributes(operationId, controller, controller->HasProgress(), shouldUpdateLightOperationAttributes);
        if (shouldUpdateLightOperationAttributes) {
            controller->SetLightOperationAttributesUpdated();
        }
    }

    void UpdateProgressAndLightAttributes(
        TOperationId operationId,
        const IOperationControllerPtr& controller,
        bool shouldUpdateProgress,
        bool shouldUpdateLightOperationAttributes)
    {
        YT_LOG_DEBUG(
            "Updating operation progress and failed jobs existence "
            "(OperationId: %v, ShouldUpdateProgress: %v, ShouldUpdateLightOperationAttributes: %v)",
            operationId,
            shouldUpdateProgress,
            shouldUpdateLightOperationAttributes);

        auto batchReq = StartObjectBatchRequestWithPrerequisites();
        GenerateMutationId(batchReq);

        auto operationPath = GetOperationPath(operationId);

        auto multisetReq = TYPathProxy::MultisetAttributes(operationPath + "/@");

        bool hasSubrequests = false;

        if (shouldUpdateProgress) {
            controller->SetProgressAttributesUpdated();
            auto progress = controller->GetProgress();
            YT_VERIFY(progress);
            auto briefProgress = controller->GetBriefProgress();
            YT_VERIFY(briefProgress);
            ValidateYson(progress, GetYsonNestingLevelLimit());
            ValidateYson(briefProgress, GetYsonNestingLevelLimit());

            bool archiveUpdated = false;
            if (Config_->EnableOperationProgressArchivation && DoesOperationsArchiveExist()) {
                archiveUpdated = TryUpdateOperationProgressInArchive(operationId, progress, briefProgress);
            }

            if (!archiveUpdated) {
                hasSubrequests = true;

                auto progressReq = multisetReq->add_subrequests();
                progressReq->set_attribute("progress");
                progressReq->set_value(progress.ToString());

                auto briefProgressReq = multisetReq->add_subrequests();
                briefProgressReq->set_attribute("brief_progress");
                briefProgressReq->set_value(briefProgress.ToString());
            }
        }

        if (shouldUpdateLightOperationAttributes) {
            hasSubrequests = true;

            bool operationHasFailedJobs = controller->GetFailedJobCount() > 0;
            auto hasFailedJobsReq = multisetReq->add_subrequests();
            hasFailedJobsReq->set_attribute("has_failed_jobs");
            hasFailedJobsReq->set_value(ConvertToYsonStringNestingLimited(operationHasFailedJobs).ToString());
        }

        if (hasSubrequests) {
            batchReq->AddRequest(multisetReq, "update_op_node");
            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }

        YT_LOG_DEBUG("Operation progress and failed jobs existence updated (OperationId: %v)",
            operationId);
    }

    bool TryUpdateOperationProgressInArchive(
        TOperationId operationId,
        const TYsonString& progress,
        const TYsonString& briefProgress)
    {
        const auto& client = Bootstrap_->GetClient();
        auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, TTransactionStartOptions{}))
            .ValueOrThrow();
        YT_LOG_DEBUG("Operation progress update transaction started (TransactionId: %v, OperationId: %v)",
            transaction->GetId(),
            operationId);

        auto progressString = progress.ToString();
        auto briefProgressString = briefProgress.ToString();

        TOrderedByIdTableDescriptor tableDescriptor;
        TUnversionedRowBuilder builder;
        auto operationIdAsGuid = operationId.Underlying();
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[0], tableDescriptor.Index.IdHi));
        builder.AddValue(MakeUnversionedUint64Value(operationIdAsGuid.Parts64[1], tableDescriptor.Index.IdLo));
        builder.AddValue(MakeUnversionedAnyValue(progressString, tableDescriptor.Index.Progress));
        builder.AddValue(MakeUnversionedAnyValue(briefProgressString, tableDescriptor.Index.BriefProgress));

        auto rowBuffer = New<TRowBuffer>();
        auto row = rowBuffer->CaptureRow(builder.GetRow());
        i64 orderedByIdRowsDataWeight = GetDataWeight(row);

        transaction->WriteRows(
            GetOperationsArchiveOrderedByIdPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(TCompactVector<TUnversionedRow, 1>{1, row}, std::move(rowBuffer)));

        auto error = WaitFor(transaction->Commit()
            .ToUncancelable()
            .WithTimeout(Config_->OperationProgressArchivationTimeout));

        if (!error.IsOK()) {
            YT_LOG_WARNING(
                error,
                "Operation progress update in archive failed (TransactionId: %v, OperationId: %v)",
                transaction->GetId(),
                operationId);
            UpdateOperationProgressFailuresCounter_.Increment();
        } else {
            YT_LOG_DEBUG("Operation progress updated successfully (TransactionId: %v, DataWeight: %v, OperationId: %v)",
                transaction->GetId(),
                orderedByIdRowsDataWeight,
                operationId);
        }
        return error.IsOK();
    }

    void AttachLivePreviewChunks(
        TOperationId operationId,
        TTransactionId transactionId,
        const std::vector<TLivePreviewRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (requests.empty()) {
            return;
        }

        struct TTableInfo
        {
            TNodeId TableId;
            TCellTag ExternalCellTag;
            std::vector<TChunkId> ChildIds;
            TTransactionId UploadTransactionId;
            TChunkListId UploadChunkListId;
            NChunkClient::NProto::TDataStatistics Statistics;
        };

        THashMap<TNodeId, TTableInfo> nodeIdToTableInfo;
        for (const auto& request : requests) {
            auto& tableInfo = nodeIdToTableInfo[request.TableId];
            tableInfo.TableId = request.TableId;
            tableInfo.ChildIds.push_back(request.ChildId);

            YT_LOG_DEBUG("Appending live preview chunk trees (OperationId: %v, TableId: %v, ChildCount: %v)",
                operationId,
                tableInfo.TableId,
                tableInfo.ChildIds.size());
        }

        THashMap<TCellTag, std::vector<TTableInfo*>> nativeCellTagToTableInfos;
        for (auto& [nodeId, tableInfo] : nodeIdToTableInfo) {
            nativeCellTagToTableInfos[CellTagFromId(nodeId)].push_back(&tableInfo);
        }

        // BeginUpload
        for (const auto& [cellTag, tableInfos] : nativeCellTagToTableInfos) {
            auto batchReq = StartObjectBatchRequestWithPrerequisites(EMasterChannelKind::Leader, cellTag);

            for (const auto* tableInfo : tableInfos) {
                auto req = TTableYPathProxy::BeginUpload(FromObjectId(tableInfo->TableId));
                req->set_update_mode(static_cast<int>(EUpdateMode::Append));
                req->set_lock_mode(static_cast<int>(ELockMode::Shared));
                req->set_upload_transaction_title(Format("Attaching live preview chunks of operation %v",
                    operationId));
                SetTransactionId(req, transactionId);
                GenerateMutationId(req);
                batchReq->AddRequest(req, "begin_upload");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto rsps = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspBeginUpload>("begin_upload");
            int rspIndex = 0;
            for (auto* tableInfo : tableInfos) {
                const auto& rsp = rsps[rspIndex++].Value();
                tableInfo->ExternalCellTag = FromProto<TCellTag>(rsp->cell_tag());
                tableInfo->UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            }
        }

        THashMap<TCellTag, std::vector<TTableInfo*>> externalCellTagToTableInfos;
        for (auto& [nodeId, tableInfo] : nodeIdToTableInfo) {
            externalCellTagToTableInfos[tableInfo.ExternalCellTag].push_back(&tableInfo);
        }

        // GetUploadParams
        for (const auto& [cellTag, tableInfos] : externalCellTagToTableInfos) {
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
        for (const auto& [cellTag, tableInfos] : externalCellTagToTableInfos) {
            auto batchReq = StartChunkBatchRequest(cellTag);
            GenerateMutationId(batchReq);
            SetSuppressUpstreamSync(&batchReq->Header(), true);
            // COMPAT(shakurov): prefer proto ext (above).
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
            for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
                auto* tableInfo = tableInfos[tableIndex];
                const auto& rsp = rsps.Get(tableIndexToRspIndex[tableIndex]);
                tableInfo->Statistics = rsp.statistics();
            }
        }

        // EndUpload
        for (const auto& [cellTag, tableInfos] : nativeCellTagToTableInfos) {
            auto batchReq = StartObjectBatchRequestWithPrerequisites(EMasterChannelKind::Leader, cellTag);

            for (const auto* tableInfo : tableInfos) {
                auto req = TTableYPathProxy::EndUpload(FromObjectId(tableInfo->TableId));
                *req->mutable_statistics() = tableInfo->Statistics;
                SetTransactionId(req, tableInfo->UploadTransactionId);
                GenerateMutationId(req);
                batchReq->AddRequest(req, "end_upload");
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

        auto* update = OperationNodesAndArchiveUpdateExecutor_->FindUpdate(operationId);
        if (!update) {
            YT_LOG_DEBUG("Trying to attach live preview to an unknown operation (OperationId: %v)",
                operationId);
            return;
        }

        // NB: Controller must attach all live preview chunks under the same transaction.
        YT_VERIFY(!update->LivePreviewTransactionId || update->LivePreviewTransactionId == transactionId);
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
        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

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

    void BuildSnapshot()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Config_->EnableSnapshotBuilding) {
            return;
        }

        TOperationIdToWeakControllerMap weakControllerMap;

        {
            const auto& controllerAgent = Bootstrap_->GetControllerAgent();
            auto controllerMap = controllerAgent->GetOperations();
            for (const auto& [operationId, operation] : controllerMap) {
                weakControllerMap.insert({operationId, operation->GetController()});
            }
        }

        auto builder = New<TSnapshotBuilder>(
            Config_,
            Bootstrap_->GetClient(),
            Bootstrap_->GetControllerAgent()->GetSnapshotIOInvoker(),
            Bootstrap_->GetControllerAgent()->GetIncarnationId(),
            ForkCounters_);

        // NB: Result is logged in the builder.
        auto error = WaitFor(builder->Run(weakControllerMap));
        if (error.IsOK()) {
            YT_LOG_INFO("Snapshot builder finished");
        } else {
            YT_LOG_ERROR(error, "Error building snapshots");
        }
    }

    void DoUpdateAccountResourceUsageLease(
        NSecurityClient::TAccountResourceUsageLeaseId leaseId,
        const TDiskQuota& diskQuota)
    {
        auto mediumDirectory = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto batchReq = proxy.ExecuteBatch();
        for (auto [mediumIndex, diskSpace] : diskQuota.DiskSpacePerMedium) {
            auto* mediumDescriptor = mediumDirectory->FindByIndex(mediumIndex);
            auto req = TYPathProxy::Set(Format("#%v/@resource_usage/disk_space_per_medium/%v", leaseId, mediumDescriptor->Name));
            req->set_value(ConvertToYsonStringNestingLimited(diskSpace).ToString());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        GetCumulativeError(batchRspOrError)
            .ThrowOnError();
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

        for (auto& [cellTag, unstageRequests] : CellTagToUnstageList_) {
            if (unstageRequests.empty()) {
                continue;
            }

            TChunkServiceProxy proxy(Bootstrap_
                ->GetClient()
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

            YT_UNUSED_FUTURE(batchReq->Invoke().Apply(
                BIND([=, cellTag = cellTag] (const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                    if (!batchRspOrError.IsOK()) {
                        YT_LOG_DEBUG(batchRspOrError, "Error unstaging chunk trees (CellTag: %v)", cellTag);
                    }
                })));
        }
    }

    void DoUpdateConfig(TControllerAgentConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config_ = config;

        if (OperationNodesAndArchiveUpdateExecutor_) {
            OperationNodesAndArchiveUpdateExecutor_->SetPeriod(Config_->OperationsUpdatePeriod);
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
        if (UpdateIntermediateMediumUsageExecutor_) {
            UpdateIntermediateMediumUsageExecutor_->SetPeriod(Config_->IntermediateMediumUsageUpdatePeriod);
        }
    }

    void ValidateConfig()
    {
        // First reset the alerts.
        SetControllerAgentAlert(EControllerAgentAlertType::UnrecognizedConfigOptions, TError());
        SetControllerAgentAlert(EControllerAgentAlertType::SnapshotLoadingDisabled, TError());

        if (Config_->EnableUnrecognizedAlert) {
            auto unrecognized = Config_->GetRecursiveUnrecognized();
            if (unrecognized && unrecognized->GetChildCount() > 0) {
                YT_LOG_WARNING("Controller agent config contains unrecognized options (Unrecognized: %v)",
                    ConvertToYsonString(unrecognized, EYsonFormat::Text));
                SetControllerAgentAlert(
                    EControllerAgentAlertType::UnrecognizedConfigOptions,
                    TError("Controller agent config contains unrecognized options")
                        << TErrorAttribute("unrecognized", unrecognized));
            }
        }

        if (!Config_->EnableSnapshotLoading && Config_->EnableSnapshotLoadingDisabledAlert) {
            auto error = TError("Snapshot loading is disabled; consider enabling it using the controller agent config");
            YT_LOG_WARNING(error);
            SetControllerAgentAlert(EControllerAgentAlertType::SnapshotLoadingDisabled, error);
        }

        if (!Config_->EnableSnapshotBuilding && Config_->EnableSnapshotBuildingDisabledAlert) {
            auto error = TError("Snapshot building is disabled; consider enabling it using the controller agent config");
            YT_LOG_WARNING(error);
            SetControllerAgentAlert(EControllerAgentAlertType::SnapshotBuildingDisabled, error);
        }
    }

    void ExecuteUpdateConfig()
    {
        YT_LOG_INFO("Updating controller agent configuration");

        try {
            auto proxy = CreateObjectServiceReadProxy(
                Bootstrap_->GetClient(),
                EMasterChannelKind::Follower);
            auto req = TYPathProxy::Get("//sys/controller_agents/config");
            auto rspOrError = WaitFor(proxy.Execute(req));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_INFO("No configuration found in Cypress");
                SetControllerAgentAlert(EControllerAgentAlertType::UnrecognizedConfigOptions, TError());
                SetControllerAgentAlert(EControllerAgentAlertType::UpdateConfig, TError());
                return;
            }

            TControllerAgentConfigPtr newConfig;
            try {
                const auto& rsp = rspOrError.ValueOrThrow();

                auto newConfigNode = PatchNode(CloneNode(InitialConfigNode_), ConvertToNode(TYsonString(rsp->value())));
                newConfig = ConvertTo<TControllerAgentConfigPtr>(newConfigNode);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error loading controller agent configuration")
                    << ex;
            }

            SetControllerAgentAlert(EControllerAgentAlertType::UpdateConfig, TError());
            auto incrementRevision = Finally([this] {
                ++ConfigRevision_;
            });

            auto oldConfigNode = ConvertToNode(Config_);
            auto newConfigNode = ConvertToNode(newConfig);
            if (AreNodesEqual(oldConfigNode, newConfigNode)) {
                YT_LOG_INFO("Controller agent configuration is not changed");
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

    void UpdateIntermediateMediumUsage()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        THashMap<TOperationId, std::pair<TTransactionId, TString>> transactions;
        for (const auto& [operationId, operation] : controllerAgent->GetOperations()) {
            auto [transaction, medium] = operation->GetController()->GetIntermediateMediumTransaction();
            if (transaction) {
                transactions[operationId] = {transaction->GetId(), medium};
            }
        }

        THashMap<TCellTag, TObjectServiceProxy::TReqExecuteBatchPtr> batchReqs;
        for (const auto& [operationId, transactionIdAndMedium] : transactions) {
            const auto& [transactionId, _] = transactionIdAndMedium;

            auto cellTag = CellTagFromId(transactionId);
            if (!batchReqs.contains(cellTag)) {
                auto proxy = CreateObjectServiceReadProxy(
                    Bootstrap_->GetClient(),
                    EMasterChannelKind::Follower,
                    cellTag);
                batchReqs[cellTag] = proxy.ExecuteBatch();
            }

            YT_LOG_DEBUG("Requesting intermediate medium usage (OperationId: %v, TransactionId: %v)",
                operationId,
                transactionId);
            auto intermediateMediumReq = TYPathProxy::Get(Format("#%v/@resource_usage", transactionId));
            batchReqs[cellTag]->AddRequest(intermediateMediumReq, ToString(transactionId));
        }

        YT_LOG_INFO("Fetching intermediate medium resource usage (OperationCount: %v, CellCount: %v)",
            transactions.size(),
            batchReqs.size());

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncBatchRsps;
        std::vector<TCellTag> cellTags;
        for (const auto& [cellTag, batchReq] : batchReqs) {
            asyncBatchRsps.push_back(batchReq->Invoke());
            cellTags.push_back(cellTag);
        }

        THashMap<TCellTag, NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> batchRsps;
        for (const auto& [cellTag, asyncBatchRsp] : Zip(cellTags, asyncBatchRsps)) {
            auto batchRspOrError = WaitFor(asyncBatchRsp);
            if (batchRspOrError.IsOK()) {
                batchRsps[cellTag] = batchRspOrError.Value();
            } else {
                YT_LOG_ERROR(batchRspOrError, "Error fetching intermediate medium resource usage (CellTag: %v)",
                    cellTag);
            }
        }

        i64 switchedOperationCount = 0;
        i64 operationCount = 0;

        for (const auto& [operationId, transactionIdAndMedium] : transactions) {
            const auto& [transactionId, medium] = transactionIdAndMedium;

            auto cellTag = CellTagFromId(transactionId);
            auto it = batchRsps.find(cellTag);
            if (it == batchRsps.end()) {
                continue;
            }

            auto intermediateMediumRsp = it->second->GetResponse<TYPathProxy::TRspGet>(ToString(transactionId));
            if (!intermediateMediumRsp.IsOK()) {
                YT_LOG_DEBUG(intermediateMediumRsp,
                    "Failed to get intermediate medium resource usage (OperationId: %v, TransactionId: %v)",
                    operationId,
                    transactionId);
                continue;
            }

            auto resourceUsage = intermediateMediumRsp.ValueOrThrow()->value();
            auto usage = TryGetInt64(
                resourceUsage,
                Format("/%v/disk_space_per_medium/%v",
                    ToYPathLiteral(NSecurityClient::IntermediateAccountName),
                    ToYPathLiteral(medium)));

            if (usage) {
                YT_LOG_DEBUG("Updating intermediate medium usage (OperationId: %v, TransactionId: %v, Usage: %v)",
                    operationId,
                    transactionId,
                    *usage);

                ++operationCount;

                if (auto operation = controllerAgent->FindOperation(operationId)) {
                    auto controller = operation->GetController();
                    YT_VERIFY(controller);

                    controller->UpdateIntermediateMediumUsage(*usage);
                    bool switchedToSlowMedium = controller->GetIntermediateMediumTransaction().first == nullptr;
                    if (switchedToSlowMedium) {
                        ++switchedOperationCount;
                    }
                } else {
                    YT_LOG_DEBUG("Intermediate medium usage updated for a unknown operation (OperationId: %v, Usage: %v)",
                        operationId,
                        *usage);
                }
            } else {
                YT_LOG_DEBUG("Intermediate medium is not used yet (OperationId: %v, TransactionId: %v)",
                    operationId,
                    transactionId);
            }
        }

        YT_LOG_INFO("Updated intermediate medium usage (OperationCount: %v, SwitchedToSlowMedium: %v)",
            operationCount,
            switchedOperationCount);
    }

    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IsConnected());

        std::vector<TError> alerts;
        for (auto alertType : TEnumTraits<EControllerAgentAlertType>::GetDomainValues()) {
            const auto& alert = Alerts_[alertType];
            if (!alert.IsOK()) {
                alerts.push_back(alert);
            }
        }

        auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetClient());
        auto req = TYPathProxy::Set(GetInstancePath() + "/@alerts");
        req->set_value(ConvertToYsonStringNestingLimited(alerts).ToString());
        req->set_recursive(true);

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error updating controller agent alerts");
        }
    }

    IAttributeDictionaryPtr CreateEphemeralAttributesNestingLimited() const
    {
        const auto nestingLevelLimit = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetConfig()
            ->CypressWriteYsonNestingLevelLimit;
        return NYTree::CreateEphemeralAttributes(nestingLevelLimit);
    }

    int GetYsonNestingLevelLimit() const
    {
        return Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetConfig()
            ->CypressWriteYsonNestingLevelLimit;
    }

    template <typename T>
    TYsonString ConvertToYsonStringNestingLimited(const T& value) const
    {
        return NYson::ConvertToYsonStringNestingLimited(value, GetYsonNestingLevelLimit());
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TControllerAgentConfigPtr config,
    INodePtr configNode,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), std::move(configNode), bootstrap))
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

TFuture<void> TMasterConnector::FlushOperationNode(TOperationId operationId)
{
    return Impl_->FlushOperationNode(operationId);
}

TFuture<void> TMasterConnector::UpdateInitializedOperationNode(TOperationId operationId, bool isCleanOperationStart)
{
    return Impl_->UpdateInitializedOperationNode(operationId, isCleanOperationStart);
}

TFuture<void> TMasterConnector::UpdateControllerFeatures(TOperationId operationId, const TYsonString& featureYson)
{
    return Impl_->UpdateControllerFeatures(operationId, featureYson);
}

TFuture<void> TMasterConnector::AttachToLivePreview(
    TOperationId operationId,
    TTransactionId transactionId,
    TNodeId tableId,
    const std::vector<TChunkTreeId>& childIds)
{
    return Impl_->AttachToLivePreview(operationId, transactionId, tableId, childIds);
}

TFuture<void> TMasterConnector::UpdateAccountResourceUsageLease(
    NSecurityClient::TAccountResourceUsageLeaseId leaseId,
    const TDiskQuota& diskQuota)
{
    return Impl_->UpdateAccountResourceUsageLease(leaseId, diskQuota);
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

ui64 TMasterConnector::GetConfigRevision() const
{
    return Impl_->GetConfigRevision();
}

bool TMasterConnector::TagsLoaded() const
{
    return Impl_->TagsLoaded();
}

const std::vector<TString>& TMasterConnector::GetTags() const
{
    return Impl_->GetTags();
}

void TMasterConnector::SetControllerAgentAlert(EControllerAgentAlertType alertType, const TError& alert)
{
    Impl_->SetControllerAgentAlert(alertType, alert);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent


