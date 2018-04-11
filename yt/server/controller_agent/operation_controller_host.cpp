#include "operation_controller_host.h"
#include "master_connector.h"
#include "controller_agent.h"
#include "operation.h"
#include "bootstrap.h"
#include "private.h"

namespace NYT {
namespace NControllerAgent {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

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

void TOperationControllerHost::InterruptJob(const TJobId& jobId, EInterruptReason reason)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Interrupted,
        jobId,
        {},
        reason
    });
    LOG_DEBUG("Job interrupt request enqueued (OperationId: %v, JobCount: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::AbortJob(const TJobId& jobId, const TError& error)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Aborted,
        jobId,
        error,
        {}
    });
    LOG_DEBUG("Job abort request enqueued (OperationId: %v, JobId: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::FailJob(const TJobId& jobId)
{
    JobEventsOutbox_->Enqueue(TAgentToSchedulerJobEvent{
        EAgentToSchedulerJobEventType::Failed,
        jobId,
        {},
        {}
    });
    LOG_DEBUG("Job failure request enqueued (OperationId: %v, JobId: %v)",
        OperationId_,
        jobId);
}

void TOperationControllerHost::ReleaseJobs(const std::vector<TJobId>& jobIds)
{
    std::vector<TAgentToSchedulerJobEvent> events;
    events.reserve(jobIds.size());
    for (const auto& jobId : jobIds) {
        events.emplace_back(TAgentToSchedulerJobEvent{
            EAgentToSchedulerJobEventType::Released,
            jobId,
            {},
            {}
        });
    }
    JobEventsOutbox_->Enqueue(std::move(events));
    LOG_DEBUG("Jobs release request enqueued (OperationId: %v, JobCount: %v)",
        OperationId_,
        jobIds.size());
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

void TOperationControllerHost::CreateJobNode(const TCreateJobNodeRequest& request)
{
    CancelableControlInvoker_->Invoke(BIND(
        &NControllerAgent::TMasterConnector::CreateJobNode,
        Bootstrap_->GetControllerAgent()->GetMasterConnector(),
        OperationId_,
        request));
}

TFuture<void> TOperationControllerHost::AttachChunkTreesToLivePreview(
    const NTransactionClient::TTransactionId& transactionId,
    const NCypressClient::TNodeId& tableId,
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

const NApi::INativeClientPtr& TOperationControllerHost::GetClient()
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

const NEventLog::TEventLogWriterPtr& TOperationControllerHost::GetEventLogWriter()
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

int TOperationControllerHost::GetExecNodeCount()
{
    return Bootstrap_->GetControllerAgent()->GetExecNodeCount();
}

TRefCountedExecNodeDescriptorMapPtr TOperationControllerHost::GetExecNodeDescriptors(const TSchedulingTagFilter& filter)
{
    return Bootstrap_->GetControllerAgent()->GetExecNodeDescriptors(filter);
}

TInstant TOperationControllerHost::GetConnectionTime()
{
    return Bootstrap_->GetControllerAgent()->GetConnectionTime();
}

const TIncarnationId& TOperationControllerHost::GetIncarnationId()
{
    return IncarnationId_;
}

const NConcurrency::IThroughputThrottlerPtr& TOperationControllerHost::GetJobSpecSliceThrottler()
{
    return Bootstrap_->GetControllerAgent()->GetJobSpecSliceThrottler();
}

void TOperationControllerHost::OnOperationCompleted()
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Completed,
        OperationId_,
        {}
    });
    LOG_DEBUG("Operation completion notification enqueued (OperationId: %v)",
        OperationId_);
}

void TOperationControllerHost::OnOperationAborted(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Aborted,
        OperationId_,
        error
    });
    LOG_DEBUG(error, "Operation abort notification enqueued (OperationId: %v)",
        OperationId_,
        error);
}

void TOperationControllerHost::OnOperationFailed(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Failed,
        OperationId_,
        error
    });
    LOG_DEBUG(error, "Operation failure notification enqueued (OperationId: %v)",
        OperationId_);
}

void TOperationControllerHost::OnOperationSuspended(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Suspended,
        OperationId_,
        error
    });
    LOG_DEBUG(error, "Operation suspension notification enqueued (OperationId: %v)",
        OperationId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
