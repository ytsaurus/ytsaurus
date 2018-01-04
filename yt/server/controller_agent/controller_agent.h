#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/event_log/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/ref.h>

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
    ~TControllerAgent();

    // XXX(babenko): remove this after getting rid of AttachJobContext
    const IInvokerPtr& GetCancelableInvoker();

    const IInvokerPtr& GetControllerThreadPoolInvoker();
    const IInvokerPtr& GetSnapshotIOInvoker();

    /*!
     *  \note Thread affinity: any
     */
    void ValidateConnected() const;
    /*!
     *  \note Thread affinity: any
     */
    TInstant GetConnectionTime() const;

    // XXX(babenko)
    TMasterConnector* GetMasterConnector();

    const TControllerAgentConfigPtr& GetConfig() const;
    void UpdateConfig(const TControllerAgentConfigPtr& config);

    const NApi::INativeClientPtr& GetMasterClient() const;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory();
    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const;
    const TCoreDumperPtr& GetCoreDumper() const;
    const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() const;
    const NEventLog::TEventLogWriterPtr& GetEventLogWriter() const;

    // XXX(babenko): any
    void RegisterController(const TOperationId& operationId, const IOperationControllerPtr& controller);
    // XXX(babenko): any
    void UnregisterController(const TOperationId& operationId);
    // XXX(babenko): any
    IOperationControllerPtr FindController(const TOperationId& operationId);
    // XXX(babenko): any
    TOperationIdToControllerMap GetControllers();

    std::vector<TErrorOr<TSharedRef>> GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests);

    void BuildOperationInfo(
        const TOperationId& operationId,
        NScheduler::NProto::TRspGetOperationInfo* response);

    NYson::TYsonString BuildJobInfo(
        const TOperationId& operationId,
        const TJobId& jobId);

    // XXX(babenko)
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

    // XXX(babenko): this method does not belong here
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
