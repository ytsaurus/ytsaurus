#pragma once

#include "private.h"

#include "data_flow_graph.h"
#include "table.h"
#include "extended_job_resources.h"

#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <expected>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Interface defining the interaction between task and controller.
struct ITaskHost
    : public virtual TRefCounted
    , public virtual IPersistent
{
    virtual IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;
    virtual IInvokerPtr GetJobSpecBuildInvoker() const = 0;

    //! Called to extract stderr table path from the spec.
    virtual std::optional<NYPath::TRichYPath> GetStderrTablePath() const = 0;
    //! Called to extract core table path from the spec.
    virtual std::optional<NYPath::TRichYPath> GetCoreTablePath() const = 0;
    //! Called to extract `enable_cuda_gpu_core_dump' from the spec.
    virtual bool GetEnableCudaGpuCoreDump() const = 0;

    virtual void UpdateTask(TTask* task) = 0;

    //! Account currently building job specs. This is used to implement ShouldSkipScheduleAllocationRequest() controller method.
    /*!
     * \note Invoker affinity: any
     */
    virtual void AccountBuildingJobSpecDelta(int countDelta, i64 totalSliceCountDelta) noexcept = 0;

    // TODO(max42): split this function into purely controller part and task part.
    virtual void InitUserJobSpecTemplate(
        NControllerAgent::NProto::TUserJobSpec* proto,
        const NScheduler::TUserJobSpecPtr& jobSpecConfig,
        const std::vector<TUserFile>& files,
        const TString& debugArtifactsAccount) = 0;
    // TODO(max42): get rid of this; serialize files either in tasks or in controller.
    virtual const std::vector<TUserFile>& GetUserFiles(const NScheduler::TUserJobSpecPtr& userJobSpec) const = 0;

    /*!
     *  \note Invoker affinity: JobSpecBuildInvoker.
     */
    virtual void CustomizeJobSpec(const TJobletPtr& joblet, NControllerAgent::NProto::TJobSpec* jobSpec) const = 0;
    virtual void CustomizeJoblet(const TJobletPtr& joblet) = 0;

    virtual void AddValueToEstimatedHistogram(const TJobletPtr& joblet) = 0;
    virtual void RemoveValueFromEstimatedHistogram(const TJobletPtr& joblet) = 0;

    virtual const TControllerAgentConfigPtr& GetConfig() const = 0;
    virtual const TOperationSpecBasePtr& GetSpec() const = 0;
    virtual const TOperationOptionsPtr& GetOptions() const = 0;

    virtual bool IsCompleted() const = 0;

    virtual void OnOperationCompleted(bool interrupted) = 0;
    virtual void OnOperationFailed(const TError& error, bool flush = true, bool abortAllJoblets = true) = 0;

    //! If |true| then all jobs started within the operation must
    //! preserve row count. This invariant is checked for each completed job.
    //! Should a violation be discovered, the operation fails.
    virtual bool IsRowCountPreserved() const = 0;
    virtual bool ShouldSkipSanityCheck() = 0;

    virtual const TChunkListPoolPtr& GetOutputChunkListPool() const = 0;
    virtual NChunkClient::TChunkListId ExtractOutputChunkList(NObjectClient::TCellTag cellTag) = 0;
    virtual NChunkClient::TChunkListId ExtractDebugChunkList(NObjectClient::TCellTag cellTag) = 0;
    virtual void ReleaseChunkTrees(
        const std::vector<NChunkClient::TChunkListId>& chunkListIds,
        bool unstageRecursively = true,
        bool waitForSnapshot = false) = 0;
    virtual void ReleaseIntermediateStripeList(const NChunkPools::TChunkStripeListPtr& stripeList) = 0;

    virtual TOperationId GetOperationId() const = 0;
    virtual EOperationType GetOperationType() const = 0;

    virtual const std::string& GetAuthenticatedUser() const = 0;

    virtual const TOutputTablePtr& StderrTable() const = 0;
    virtual const TOutputTablePtr& CoreTable() const = 0;

    virtual void RegisterStderr(const TJobletPtr& joblet, const TJobSummary& summary) = 0;
    virtual void RegisterCores(const TJobletPtr& joblet, const TJobSummary& summary) = 0;

    virtual void RegisterJoblet(const TJobletPtr& joblet) = 0;
    virtual std::expected<TJobId, EScheduleFailReason> GenerateJobId(NScheduler::TAllocationId allocationId, TJobId previousJobId) = 0;

    virtual std::optional<TJobMonitoringDescriptor> RegisterJobForMonitoring(TJobId jobId) = 0;

    virtual const std::optional<TJobResources>& CachedMaxAvailableExecNodeResources() const = 0;

    virtual TInputManagerPtr GetInputManager() const = 0;

    virtual void RegisterRecoveryInfo(
        const TCompletedJobPtr& completedJob,
        const NChunkPools::TChunkStripePtr& stripe) = 0;

    virtual TExtendedJobResources GetAutoMergeResources(
        const NTableClient::TChunkStripeStatisticsVector& statistics) const = 0;
    virtual TAutoMergeDirector* GetAutoMergeDirector() = 0;

    virtual const std::vector<TOutputStreamDescriptorPtr>& GetStandardStreamDescriptors() const = 0;

    virtual NTableClient::TRowBufferPtr GetRowBuffer() = 0;

    virtual void AttachToIntermediateLivePreview(NChunkClient::TInputChunkPtr chunk) = 0;

    virtual void RegisterTeleportChunk(
        NChunkClient::TInputChunkPtr chunkSpec,
        NChunkPools::TChunkStripeKey key,
        int tableIndex) = 0;

    virtual const TDataFlowGraphPtr& GetDataFlowGraph() const = 0;

    virtual void RegisterLivePreviewChunk(
        const TDataFlowGraph::TVertexDescriptor& vertexDescriptor,
        int index,
        const NChunkClient::TInputChunkPtr& chunk) = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const = 0;

    virtual TJobletPtr CreateJoblet(
        TTask* task,
        TJobId jobId,
        TString treeId,
        int taskJobIndex,
        std::optional<TString> poolPath,
        bool treeIsTentative) = 0;

    virtual TSharedRef BuildJobSpecProto(
        const TJobletPtr& joblet,
        const std::optional<NScheduler::NProto::TScheduleAllocationSpec>& scheduleAllocationSpec) = 0;

    virtual void RegisterOutputTables(const std::vector<NYPath::TRichYPath>& outputTablePaths) = 0;

    virtual void AsyncAbortJob(TJobId jobId, EAbortReason abortReason) = 0;
    virtual void AbortJob(TJobId jobId, EAbortReason abortReason) = 0;

    virtual bool CanInterruptJobs() const = 0;
    virtual void InterruptJob(TJobId jobId, EInterruptionReason reason) = 0;

    virtual void OnCompetitiveJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitionType) = 0;

    virtual const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const = 0;

    //! Joins job splitter config from the job spec with job splitter config
    //! from the controller agent config and returns the result.
    virtual TJobSplitterConfigPtr GetJobSplitterConfigTemplate() const = 0;

    virtual const TInputTablePtr& GetInputTable(int tableIndex) const = 0;
    virtual const TOutputTablePtr& GetOutputTable(int tableIndex) const = 0;
    virtual int GetOutputTableCount() const = 0;

    virtual void SetOperationAlert(EOperationAlertType type, const TError& alert) = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;

    virtual const std::vector<TString>& GetOffloadingPoolTrees() = 0;
    virtual TJobExperimentBasePtr GetJobExperiment() = 0;

    virtual bool IsIdleCpuPolicyAllowedInTree(const TString& treeId) const = 0;

    virtual bool IsTreeProbing(const TString& treeId) const = 0;

    virtual std::shared_ptr<const THashMap<NScheduler::TClusterName, bool>> GetClusterToNetworkBandwidthAvailability() const = 0;

    virtual bool IsNetworkBandwidthAvailable(const NScheduler::TClusterName& clusterName) const = 0;

    virtual void SubscribeToClusterNetworkBandwidthAvailabilityUpdated(
        const NScheduler::TClusterName& clusterName,
        const TCallback<void()>& callback) const = 0;

    virtual void UnsubscribeFromClusterNetworkBandwidthAvailabilityUpdated(
        const NScheduler::TClusterName& clusterName,
        const TCallback<void()>& callback) const = 0;

    virtual void UpdateWriteBufferMemoryAlert(TJobId jobId, i64 currentMemory, i64 previousMemory) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITaskHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
