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
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TOperationEvent>> operationEventsOutbox,
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TJobEvent>> jobEventsOutbox,
    NCellScheduler::TBootstrap* bootstrap)
    : OperationId_(operation->GetId())
    , CancelableControlInvoker_(std::move(cancelableControlInvoker))
    , OperationEventsOutbox_(std::move(operationEventsOutbox))
    , JobEventsOutbox_(std::move(jobEventsOutbox))
    , Bootstrap_(bootstrap)
    , IncarnationId_(Bootstrap_->GetControllerAgent()->GetMasterConnector()->GetIncarnationId())
{ }

void TOperationControllerHost::InterruptJob(const TJobId& jobId, EInterruptReason reason)
{
    JobEventsOutbox_->Enqueue(TJobEvent{
        EAgentToSchedulerJobEventType::Interrupted,
        jobId,
        {},
        reason
    });
}

void TOperationControllerHost::AbortJob(const TJobId& jobId, const TError& error)
{
    JobEventsOutbox_->Enqueue(TJobEvent{
        EAgentToSchedulerJobEventType::Aborted,
        jobId,
        error,
        {}
    });
}

void TOperationControllerHost::FailJob(const TJobId& jobId)
{
    JobEventsOutbox_->Enqueue(TJobEvent{
        EAgentToSchedulerJobEventType::Failed,
        jobId,
        {},
        {}
    });
}

void TOperationControllerHost::ReleaseJobs(const std::vector<TJobId>& jobIds)
{
    std::vector<TJobEvent> events;
    events.reserve(jobIds.size());
    for (const auto& jobId : jobIds) {
        events.emplace_back(TJobEvent{
            EAgentToSchedulerJobEventType::Released,
            jobId,
            {},
            {}
        });
    }
    JobEventsOutbox_->EnqueueMany(std::move(events));
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

TExecNodeDescriptorListPtr TOperationControllerHost::GetExecNodeDescriptors(const TSchedulingTagFilter& filter)
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
    auto itemId = OperationEventsOutbox_->Enqueue(TOperationEvent{
        EAgentToSchedulerOperationEventType::Completed,
        OperationId_,
        TError()
    });
    LOG_DEBUG("Operation completion event enqueued (ItemId: %v, OperationId: %v)",
        itemId,
        OperationId_);
}

void TOperationControllerHost::OnOperationAborted(const TError& error)
{
    auto itemId = OperationEventsOutbox_->Enqueue(TOperationEvent{
        EAgentToSchedulerOperationEventType::Aborted,
        OperationId_,
        error
    });
    LOG_DEBUG("Operation abort event enqueued (ItemId: %v, OperationId: %v, Error: %v)",
        itemId,
        OperationId_,
        error);
}

void TOperationControllerHost::OnOperationFailed(const TError& error)
{
    auto itemId = OperationEventsOutbox_->Enqueue(TOperationEvent{
        EAgentToSchedulerOperationEventType::Failed,
        OperationId_,
        error
    });
    LOG_DEBUG("Operation failure event enqueued (ItemId: %v, OperationId: %v, Error: %v)",
        itemId,
        OperationId_,
        error);
}

void TOperationControllerHost::OnOperationSuspended(const TError& error)
{
    auto itemId = OperationEventsOutbox_->Enqueue(TOperationEvent{
        EAgentToSchedulerOperationEventType::Suspended,
        OperationId_,
        error
    });
    LOG_DEBUG("Operation suspension event enqueued (ItemId: %v, OperationId: %v, Error: %v)",
        itemId,
        OperationId_,
        error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
