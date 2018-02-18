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

    /*!
     *  \note Thread affinity: any
     */
    TMasterConnector* GetMasterConnector();

    void ValidateConnected() const;
    void ValidateIncarnation(const TIncarnationId& incarnationId) const;

    /*!
     *  \note Thread affinity: any
     */
    TInstant GetConnectionTime() const;

    const TControllerAgentConfigPtr& GetConfig() const;
    void UpdateConfig(const TControllerAgentConfigPtr& config);

    TOperationPtr FindOperation(const TOperationId& operationId);
    TOperationPtr GetOperation(const TOperationId& operationId);
    TOperationPtr GetOperationOrThrow(const TOperationId& operationId);
    const TOperationIdToOperationMap& GetOperations();

    void RegisterOperation(const NProto::TOperationDescriptor& descriptor);
    TFuture<TOperationControllerInitializationResult> InitializeOperation(const TOperationPtr& operation, const TNullable<TControllerTransactions>& transactions);
    TFuture<TOperationControllerPrepareResult> PrepareOperation(const TOperationPtr& operation);
    TFuture<void> MaterializeOperation(const TOperationPtr& operation);
    TFuture<TOperationControllerReviveResult> ReviveOperation(const TOperationPtr& operation);
    TFuture<void> CommitOperation(const TOperationPtr& operation);
    TFuture<void> CompleteOperation(const TOperationPtr& operation);
    TFuture<void> AbortOperation(const TOperationPtr& operation);
    TFuture<void> DisposeOperation(const TOperationPtr& operation);

    //! Extracts specs for given jobs; nulls indicate failures (e.g. missing jobs).
    TFuture<std::vector<TErrorOr<TSharedRef>>> ExtractJobSpecs(const std::vector<TJobSpecRequest>& requests);

    TFuture<TOperationInfo> BuildOperationInfo(const TOperationId& operationId);
    TFuture<NYson::TYsonString> BuildJobInfo(const TOperationId& operationId, const TJobId& jobId);

    //! Returns the total number of online exec nodes.
    /*!
     *  \note Thread affinity: any
     */
    int GetExecNodeCount() const;

    //! Returns the descriptors of online exec nodes matching a given #filter.
    /*!
     *  \note Thread affinity: any
     */
    NScheduler::TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter) const;

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
