#include "operation_controller_host.h"
#include "master_connector.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

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
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> operationEventsOutbox,
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>> jobEventsOutbox,
    NCellScheduler::TBootstrap* bootstrap)
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
    const NTransactionClient::TTransactionId& transactionId, const std::vector<NCypressClient::TNodeId>& tableIds,
    const std::vector<TChunkTreeId>& childIds)
{
    return BIND(&NControllerAgent::TMasterConnector::AttachToLivePreview, Bootstrap_->GetControllerAgent()->GetMasterConnector())
        .AsyncVia(CancelableControlInvoker_)
        .Run(
            OperationId_,
            transactionId,
            tableIds,
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
    return Bootstrap_->GetControllerAgent()->GetClient();
}

const NNodeTrackerClient::TNodeDirectoryPtr& TOperationControllerHost::GetNodeDirectory()
{
    return Bootstrap_->GetControllerAgent()->GetNodeDirectory();
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

const TCoreDumperPtr& TOperationControllerHost::GetCoreDumper()
{
    return Bootstrap_->GetControllerAgent()->GetCoreDumper();
}

const TAsyncSemaphorePtr& TOperationControllerHost::GetCoreSemaphore()
{
    return Bootstrap_->GetControllerAgent()->GetCoreSemaphore();
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
    LOG_DEBUG("Operation abort notification enqueued (OperationId: %v, Error: %v)",
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
    LOG_DEBUG("Operation failure notification enqueued (OperationId: %v, Error: %v)",
        OperationId_,
        error);
}

void TOperationControllerHost::OnOperationSuspended(const TError& error)
{
    OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent{
        EAgentToSchedulerOperationEventType::Suspended,
        OperationId_,
        error
    });
    LOG_DEBUG("Operation suspension notification enqueued (OperationId: %v, Error: %v)",
        OperationId_,
        error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
