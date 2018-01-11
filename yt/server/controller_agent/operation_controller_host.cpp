#include "operation_controller_host.h"
#include "master_connector.h"
#include "controller_agent.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/operation.h>

namespace NYT {
namespace NControllerAgent {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TOperationControllerHost::TOperationControllerHost(
    TOperation* operation,
    IInvokerPtr cancelableControlInvoker,
    NCellScheduler::TBootstrap* bootstrap)
    : OperationId_(operation->GetId())
    , CancelableControlInvoker_(std::move(cancelableControlInvoker))
    , Bootstrap_(bootstrap)
    , IncarnationId_(Bootstrap_->GetControllerAgent()->GetMasterConnector()->GetIncarnationId())
{ }

void TOperationControllerHost::InterruptJob(const TJobId& jobId, EInterruptReason reason)
{
    Bootstrap_->GetControllerAgent()->InterruptJob(IncarnationId_, jobId, reason);
}

void TOperationControllerHost::AbortJob(const TJobId& jobId, const TError& error)
{
    Bootstrap_->GetControllerAgent()->AbortJob(IncarnationId_, jobId, error);
}

void TOperationControllerHost::FailJob(const TJobId& jobId)
{
    Bootstrap_->GetControllerAgent()->FailJob(IncarnationId_, jobId);
}

void TOperationControllerHost::ReleaseJobs(const std::vector<TJobId>& jobIds)
{
    Bootstrap_->GetControllerAgent()->ReleaseJobs(IncarnationId_, jobIds);
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

TFuture<void> TOperationControllerHost::GetHeartbeatSentFuture()
{
    return Bootstrap_->GetControllerAgent()->GetHeartbeatSentFuture();
}

const NConcurrency::IThroughputThrottlerPtr& TOperationControllerHost::GetJobSpecSliceThrottler()
{
    return Bootstrap_->GetControllerAgent()->GetJobSpecSliceThrottler();
}

void TOperationControllerHost::OnOperationCompleted()
{
    auto guard = Guard(EventsLock_);
    Completed_ = true;
}

void TOperationControllerHost::OnOperationAborted(const TError& error)
{
    auto guard = Guard(EventsLock_);
    AbortError_ = error;
}

void TOperationControllerHost::OnOperationFailed(const TError& error)
{
    auto guard = Guard(EventsLock_);
    FailureError_ = error;
}

void TOperationControllerHost::OnOperationSuspended(const TError& error)
{
    auto guard = Guard(EventsLock_);
    SuspensionError_ = error;
}

TOperationControllerEvent TOperationControllerHost::PullEvent()
{
    auto guard = Guard(EventsLock_);
    if (!AbortError_.IsOK()) {
        return TOperationAbortedEvent{AbortError_};
    }
    if (!FailureError_.IsOK()) {
        return TOperationFailedEvent{FailureError_};
    }
    if (!SuspensionError_.IsOK()) {
        // NB: Suspension error is non-sticky.
        auto error = SuspensionError_;
        SuspensionError_ = {};
        return TOperationSuspendedEvent{error};
    }
    if (Completed_) {
        return TOperationCompletedEvent{};
    }
    return TNullOperationEvent{};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
