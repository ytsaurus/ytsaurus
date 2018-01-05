#pragma once

#include "operation_controller.h"

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

/*!
 *  \note Thread affinity: Control unless noted otherwise
 */
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

    /*!
     *  \note Thread affinity: any
     */
    const IInvokerPtr& GetControllerThreadPoolInvoker();
    /*!
     *  \note Thread affinity: any
     */
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

    // XXX(babenko): affinity
    const TControllerAgentConfigPtr& GetConfig() const;
    void UpdateConfig(const TControllerAgentConfigPtr& config);

    /*!
     *  \note Thread affinity: any
     */
    const NApi::INativeClientPtr& GetMasterClient() const;
    /*!
     *  \note Thread affinity: any
     */
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory();
    /*!
     *  \note Thread affinity: any
     */
    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const;
    /*!
     *  \note Thread affinity: any
     */
    const TCoreDumperPtr& GetCoreDumper() const;
    /*!
     *  \note Thread affinity: any
     */
    const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() const;
    /*!
     *  \note Thread affinity: any
     */
    const NEventLog::TEventLogWriterPtr& GetEventLogWriter() const;

    void RegisterController(const TOperationId& operationId, const IOperationControllerPtr& controller);
    void UnregisterController(const TOperationId& operationId);
    IOperationControllerPtr FindController(const TOperationId& operationId);
    TOperationIdToControllerMap GetControllers();

    // TODO(babenko): maybe relax affinity?
    std::vector<TErrorOr<TSharedRef>> GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests);

    TFuture<TOperationInfo> BuildOperationInfo(const TOperationId& operationId);
    TFuture<NYson::TYsonString> BuildJobInfo(const TOperationId& operationId, const TJobId& jobId);

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

    // XXX(babenko): check affinity
    void InterruptJob(const TJobId& jobId, EInterruptReason reason);
    // XXX(babenko): check affinity
    void AbortJob(const TJobId& jobId, const TError& error);
    // XXX(babenko): check affinity
    void FailJob(const TJobId& jobId);
    // XXX(babenko): check affinity
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
