#include "operation_controller_host.h"
#include "master_connector.h"
#include "controller_agent.h"
#include "operation.h"
#include "bootstrap.h"
#include "private.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NScheduler;
using namespace NYTree;
using NJobTrackerClient::TJobToRelease;
using NJobTrackerClient::TReleaseJobFlags;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateCompletedEvent(TOperationId operationId)
{
    return TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Completed,
        operationId
    };
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateSuspendedEvent(TOperationId operationId, TError error)
{
    return TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Suspended,
        operationId,
        std::move(error)
    };
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateFailedEvent(TOperationId operationId, TError error)
{
    return TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Failed,
        operationId,
        std::move(error)
    };
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateAbortedEvent(TOperationId operationId, TError error)
{
    return TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Aborted,
        operationId,
        std::move(error)
    };
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateBannedInTentativeTreeEvent(
    TOperationId operationId,
    TString treeId,
    std::vector<TJobId> jobIds)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::BannedInTentativeTree,
        operationId,
    };
    event.TentativeTreeId = std::move(treeId);
    event.TentativeTreeJobIds = std::move(jobIds);
    return event;
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
    TOperationId operationId,
    TError error,
    std::optional<TOperationControllerInitializeResult> maybeResult)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::InitializationFinished,
        operationId,
        std::move(error)
    };
    event.InitializeResult = maybeResult;
    return event;
}


TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
    TOperationId operationId,
    TError error,
    std::optional<TOperationControllerPrepareResult> maybeResult)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::PreparationFinished,
        operationId,
        std::move(error)
    };
    event.PrepareResult = maybeResult;
    return event;
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
    TOperationId operationId,
    TError error,
    std::optional<TOperationControllerMaterializeResult> maybeResult)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::MaterializationFinished,
        operationId,
        std::move(error)
    };
    event.MaterializeResult = maybeResult;
    return event;
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
    TOperationId operationId,
    TError error,
    std::optional<TOperationControllerReviveResult> maybeResult)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::RevivalFinished,
        operationId,
        std::move(error)
    };
    event.ReviveResult = maybeResult;
    return event;
}

TAgentToSchedulerOperationEvent TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
    TOperationId operationId,
    TError error,
    std::optional<TOperationControllerCommitResult> maybeResult)
{
    TAgentToSchedulerOperationEvent event{
        EAgentToSchedulerOperationEventType::CommitFinished,
        operationId,
        std::move(error)
    };
    event.CommitResult = maybeResult;
    return event;
}

////////////////////////////////////////////////////////////////////////////////

TOperationControllerHost::TOperationControllerHost(
    TOperation* operation,
    IInvokerPtr cancelableControlInvoker,
    TIntrusivePtr<TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> operationEventsOutbox,
    TIntrusivePtr<TMessageQueueOutbox<TAgentToSchedulerJobEvent>> jobEventsOutbox,
    TBootstrap* bootstrap)
    : OperationId_(operation->GetId())
    , CancelableControlInvoker_(std::move(cancelableControlInvoker))
    , OperationEventsOutbox_(std::move(operationEventsOutbox))
    , JobEventsOutbox_(std::move(jobEventsOutbox))
    , Bootstrap_(bootstrap)
    , IncarnationId_(Bootstrap_->GetControllerAgent()->GetIncarnationId())
{ }

void TOperationControllerHost::InterruptJob(TJobId jobId, EInterruptReason reason)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Interrupted,
        jobId,
        {},
        reason,
        {},
    });
    YT_LOG_DEBUG("Job interrupt request enqueued (OperationId: %v, JobCount: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::AbortJob(TJobId jobId, const TError& error)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Aborted,
        jobId,
        error,
        {},
        {},
    });
    YT_LOG_DEBUG("Job abort request enqueued (OperationId: %v, JobId: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::FailJob(TJobId jobId)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Failed,
        jobId,
        {},
        {},
        {},
    });
    YT_LOG_DEBUG("Job failure request enqueued (OperationId: %v, JobId: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::ReleaseJobs(const std::vector<TJobToRelease>& jobsToRelease)
{
    std::vector<TAgentToSchedulerJobEvent> events;
    events.reserve(jobsToRelease.size());
    for (const auto& jobToRelease : jobsToRelease) {
        events.emplace_back(TAgentToSchedulerJobEvent{
            EAgentToSchedulerJobEventType::Released,
            jobToRelease.JobId,
            {},
            {},
            jobToRelease.ReleaseFlags,
        });
    }
    JobEventsOutbox_->Enqueue(std::move(events));
    YT_LOG_DEBUG("Jobs release request enqueued (OperationId: %v, JobCount: %v)",
        OperationId_,
        jobsToRelease.size());
}

TFuture<TOperationSnapshot> TOperationControllerHost::DownloadSnapshot()
{
    return BIND(&NControllerAgent::TMasterConnector::DownloadSnapshot, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(OperationId_);
}

TFuture<void> TOperationControllerHost::RemoveSnapshot()
{
    return BIND(&NControllerAgent::TMasterConnector::RemoveSnapshot, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(OperationId_);
}

TFuture<void> TOperationControllerHost::FlushOperationNode()
{
    return BIND(&NControllerAgent::TMasterConnector::FlushOperationNode, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(OperationId_);
}

TFuture<void> TOperationControllerHost::UpdateInitializedOperationNode()
{
    return BIND(&NControllerAgent::TMasterConnector::UpdateInitializedOperationNode, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(OperationId_);
}

void TOperationControllerHost::CreateJobNode(const TCreateJobNodeRequest& request)
{
    CancelableControlInvoker_->Invoke(BIND(
        &NControllerAgent::TMasterConnector::CreateJobNode,
        Bootstrap_->GetControllerAgent()->GetMasterConnector(),
        OperationId_,
        request));
}

TFuture<void> TOperationControllerHost::AttachChunkTreesToLivePreview(
    NTransactionClient::TTransactionId transactionId,
    NCypressClient::TNodeId tableId,
    const std::vector<TChunkTreeId>& childIds)
{
    return BIND(&NControllerAgent::TMasterConnector::AttachToLivePreview, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(
            OperationId_,
            transactionId,
            tableId,
            childIds);
}

void TOperationControllerHost::AddChunkTreesToUnstageList(const std::vector<TChunkId>& chunkTreeIds, bool recursive)
{
    CancelableControlInvoker_->Invoke(BIND(
        &NControllerAgent::TMasterConnector::AddChunkTreesToUnstageList,
        Bootstrap_->GetControllerAgent()->GetMasterConnector(),
        chunkTreeIds,
        recursive));
}

const NApi::NNative::IClientPtr& TOperationControllerHost::GetClient()
{
    return Bootstrap_->GetMasterClient();
}

const NNodeTrackerClient::TNodeDirectoryPtr& TOperationControllerHost::GetNodeDirectory()
{
    return Bootstrap_->GetNodeDirectory();
}

const TThrottlerManagerPtr& TOperationControllerHost::GetChunkLocationThrottlerManager()
{
    return Bootstrap_->GetControllerAgent()->GetChunkLocationThrottlerManager();
}

const IInvokerPtr& TOperationControllerHost::GetControllerThreadPoolInvoker()
{
    return Bootstrap_->GetControllerAgent()->GetControllerThreadPoolInvoker();
}

const NEventLog::IEventLogWriterPtr& TOperationControllerHost::GetEventLogWriter()
{
    return Bootstrap_->GetControllerAgent()->GetEventLogWriter();
}

const ICoreDumperPtr& TOperationControllerHost::GetCoreDumper()
{
    return Bootstrap_->GetControllerAgent()->GetCoreDumper();
}

const TAsyncSemaphorePtr& TOperationControllerHost::GetCoreSemaphore()
{
    return Bootstrap_->GetControllerAgent()->GetCoreSemaphore();
}

TMemoryTagQueue* TOperationControllerHost::GetMemoryTagQueue()
{
    return Bootstrap_->GetControllerAgent()->GetMemoryTagQueue();
}

int TOperationControllerHost::GetOnlineExecNodeCount()
{
    return Bootstrap_->GetControllerAgent()->GetOnlineExecNodeCount();
}

TRefCountedExecNodeDescriptorMapPtr TOperationControllerHost::GetExecNodeDescriptors(const TSchedulingTagFilter& filter, bool onlineOnly)
{
    return Bootstrap_->GetControllerAgent()->GetExecNodeDescriptors(filter, onlineOnly);
}

TInstant TOperationControllerHost::GetConnectionTime()
{
    return Bootstrap_->GetControllerAgent()->GetConnectionTime();
}

TIncarnationId TOperationControllerHost::GetIncarnationId()
{
    return IncarnationId_;
}

const NConcurrency::IThroughputThrottlerPtr& TOperationControllerHost::GetJobSpecSliceThrottler()
{
    return Bootstrap_->GetControllerAgent()->GetJobSpecSliceThrottler();
}

const NJobAgent::TJobReporterPtr& TOperationControllerHost::GetJobReporter()
{
    return Bootstrap_->GetControllerAgent()->GetJobReporter();
}

const NChunkClient::TMediumDirectoryPtr& TOperationControllerHost::GetMediumDirectory()
{
    return Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetMediumDirectory();
}

void TOperationControllerHost::OnOperationCompleted()
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateCompletedEvent(OperationId_));
    YT_LOG_DEBUG("Operation completion notification enqueued (OperationId: %v)",
        OperationId_);
}

void TOperationControllerHost::OnOperationAborted(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateAbortedEvent(OperationId_, error));
    YT_LOG_DEBUG(error, "Operation abort notification enqueued (OperationId: %v)",
        OperationId_,
        error);
}

void TOperationControllerHost::OnOperationFailed(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateFailedEvent(OperationId_, error));
    YT_LOG_DEBUG(error, "Operation failure notification enqueued (OperationId: %v)",
        OperationId_);
}

void TOperationControllerHost::OnOperationSuspended(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateSuspendedEvent(OperationId_, error));
    YT_LOG_DEBUG(error, "Operation suspension notification enqueued (OperationId: %v)",
        OperationId_);
}

void TOperationControllerHost::OnOperationBannedInTentativeTree(const TString& treeId, const std::vector<TJobId>& jobIds)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateBannedInTentativeTreeEvent(
        OperationId_,
        treeId,
        jobIds));
    YT_LOG_DEBUG("Operation tentative tree ban notification enqueued (OperationId: %v, TreeId: %v)",
        OperationId_,
        treeId);
}

void TOperationControllerHost::ValidateOperationAccess(
    const TString& user,
    EPermission permission)
{
    WaitFor(BIND(&TControllerAgent::ValidateOperationAccess, Bootstrap_->GetControllerAgent())
        .AsyncVia(CancelableControlInvoker_)
        .Run(
            user,
            OperationId_,
            permission))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
