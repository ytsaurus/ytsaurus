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

struct TJobSpecRequest
{
    TOperationId OperationId;
    TJobId JobId;
};

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

    const TControllerAgentConfigPtr& GetConfig() const;
    void UpdateConfig(const TControllerAgentConfigPtr& config);

    /*!
     *  \note Thread affinity: any
     */
    const NApi::INativeClientPtr& GetClient() const;
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

    //! Extracts specs for given jobs; nulls indicate failures (e.g. missing jobs).
    TFuture<std::vector<TErrorOr<TSharedRef>>> ExtractJobSpecs(const std::vector<TJobSpecRequest>& requests);

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

    /*!
     *  \note Thread affinity: any
     */
    void InterruptJob(const TIncarnationId& incarnationId, const TJobId& jobId, EInterruptReason reason);
    /*!
     *  \note Thread affinity: any
     */
    void AbortJob(const TIncarnationId& incarnationId, const TJobId& jobId, const TError& error);
    /*!
     *  \note Thread affinity: any
     */
    void FailJob(const TIncarnationId& incarnationId, const TJobId& jobId);
    /*!
     *  \note Thread affinity: any
     */
    void ReleaseJobs(const TIncarnationId& incarnationId, const std::vector<TJobId>& jobIds);

    /*!
     *  \note Thread affinity: any
     */
    const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
