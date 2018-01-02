#pragma once

#include "public.h"

#include "master_connector.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/server/scheduler/scheduling_tag.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service.pb.h>
#include <yt/ytlib/job_tracker_client/job_spec_service.pb.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/scheduler/controller_agent_operation_service_proxy.h>

#include <yt/ytlib/event_log/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

class TControllerAgent
    : public TRefCounted
{
public:
    TControllerAgent(
        TControllerAgentConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    void Connect();
    void Disconnect();
    void ValidateConnected() const;

    TInstant GetConnectionTime() const;

    const IInvokerPtr& GetInvoker();
    const IInvokerPtr& GetCancelableInvoker();

    const IInvokerPtr& GetControllerThreadPoolInvoker();
    const IInvokerPtr& GetSnapshotIOInvoker();

    TMasterConnector* GetMasterConnector();

    const TControllerAgentConfigPtr& GetConfig() const;
    const NApi::INativeClientPtr& GetMasterClient() const;

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory();

    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const;

    const TCoreDumperPtr& GetCoreDumper() const;
    const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() const;

    const NEventLog::TEventLogWriterPtr& GetEventLogWriter() const;

    void UpdateConfig(const TControllerAgentConfigPtr& config);

    void RegisterOperation(const TOperationId& operationId, IOperationControllerPtr controller);
    void UnregisterOperation(const TOperationId& operationId);

    std::vector<TErrorOr<TSharedRef>> GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests);

    void BuildOperationInfo(
        const TOperationId& operationId,
        NScheduler::NProto::TRspGetOperationInfo* response);

    NYson::TYsonString BuildJobInfo(
        const TOperationId& operationId,
        const TJobId& jobId);

    TFuture<void> GetHeartbeatSentFuture();

    //! Returns the total number of online exec nodes.
    /*!
     *  \note Thread affinity: any
     */
    int GetExecNodeCount() const;

    //! Returns the descriptors of online exec nodes matching a given #filter.
    /*!
     *  \note Thread affinity: any
     */
    NScheduler::TExecNodeDescriptorListPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter) const;

    void AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);

    void InterruptJob(const TJobId& jobId, EInterruptReason reason);
    void AbortJob(const TJobId& jobId, const TError& error);
    void FailJob(const TJobId& jobId);
    void ReleaseJobs(
        std::vector<TJobId> jobIds,
        const TOperationId& operationId,
        int controllerSchedulerIncarnation);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
