#pragma once

#include "private.h"
#include "serialize.h"
#include "table.h"
#include "data_flow_graph.h"

#include <yt/server/chunk_pools/public.h>
#include <yt/server/chunk_pools/chunk_stripe.h>

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Interface defining the interaction between task and controller.
struct ITaskHost
    : public virtual TRefCounted
    , public IPersistent
    , public NPhoenix::TFactoryTag<NPhoenix::TNullFactory>
{
    virtual IInvokerPtr GetCancelableInvoker() const = 0;

    //! Called to extract stderr table path from the spec.
    virtual TNullable<NYPath::TRichYPath> GetStderrTablePath() const = 0;
    //! Called to extract core table path from the spec.
    virtual TNullable<NYPath::TRichYPath> GetCoreTablePath() const = 0;

    virtual void RegisterInputStripe(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task) = 0;
    virtual void AddTaskLocalityHint(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task) = 0;
    virtual void AddTaskLocalityHint(NNodeTrackerClient::TNodeId nodeId, const TTaskPtr& task) = 0;
    virtual void AddTaskPendingHint(const TTaskPtr& task) = 0;

    virtual ui64 NextJobIndex() = 0;

    virtual void CustomizeJobSpec(const TJobletPtr& joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;
    virtual void CustomizeJoblet(const TJobletPtr& joblet) = 0;

    virtual void AddValueToEstimatedHistogram(const TJobletPtr& joblet) = 0;
    virtual void RemoveValueFromEstimatedHistogram(const TJobletPtr& joblet) = 0;

    virtual const TControllerAgentConfigPtr& GetConfig() const = 0;
    virtual const TOperationSpecBasePtr& GetSpec() const = 0;

    virtual void OnOperationFailed(const TError& error, bool flush = true) = 0;

    //! If |true| then all jobs started within the operation must
    //! preserve row count. This invariant is checked for each completed job.
    //! Should a violation be discovered, the operation fails.
    virtual bool IsRowCountPreserved() const = 0;
    virtual bool IsJobInterruptible() const = 0;
    virtual bool ShouldSkipSanityCheck() = 0;

    virtual const IDigest* GetJobProxyMemoryDigest(EJobType jobType) const = 0;
    virtual const IDigest* GetUserJobMemoryDigest(EJobType jobType) const = 0;

    virtual NObjectClient::TCellTag GetIntermediateOutputCellTag() const = 0;

    virtual const TChunkListPoolPtr& GetChunkListPool() const = 0;
    virtual NChunkClient::TChunkListId ExtractChunkList(NObjectClient::TCellTag cellTag) = 0;
    virtual void ReleaseChunkTrees(
        const std::vector<NChunkClient::TChunkListId>& chunkListIds,
        bool unstageRecursively = true,
        bool waitForSnapshot = false) = 0;
    virtual void ReleaseIntermediateStripeList(const NChunkPools::TChunkStripeListPtr& stripeList) = 0;

    virtual TOperationId GetOperationId() const = 0;
    virtual EOperationType GetOperationType() const = 0;

    virtual const TNullable<TOutputTable>& StderrTable() const = 0;
    virtual const TNullable<TOutputTable>& CoreTable() const = 0;

    virtual void RegisterStderr(const TJobletPtr& joblet, const TJobSummary& summary) = 0;
    virtual void RegisterCores(const TJobletPtr& joblet, const TJobSummary& summary) = 0;

    virtual void RegisterJoblet(const TJobletPtr& joblet) = 0;

    virtual IJobSplitter* GetJobSplitter() = 0;

    virtual const TNullable<TJobResources>& CachedMaxAvailableExecNodeResources() const = 0;

    virtual const NNodeTrackerClient::TNodeDirectoryPtr& InputNodeDirectory() const = 0;

    virtual void RegisterRecoveryInfo(
        const TCompletedJobPtr& completedJob,
        const NChunkPools::TChunkStripePtr& stripe) = 0;

    virtual NScheduler::TExtendedJobResources GetAutoMergeResources(
        const NChunkPools::TChunkStripeStatisticsVector& statistics) const = 0;
    virtual const NJobTrackerClient::NProto::TJobSpec& GetAutoMergeJobSpecTemplate(int tableIndex) const = 0;
    virtual TTaskGroupPtr GetAutoMergeTaskGroup() const = 0;
    virtual TAutoMergeDirector* GetAutoMergeDirector() = 0;

    virtual void Persist(const TPersistenceContext& context) = 0;

    virtual const std::vector<TEdgeDescriptor>& GetStandardEdgeDescriptors() const = 0;

    virtual NTableClient::TRowBufferPtr GetRowBuffer() = 0;

    virtual void AttachToIntermediateLivePreview(NChunkClient::TChunkId chunkId) = 0;

    virtual void RegisterTeleportChunk(
        NChunkClient::TInputChunkPtr chunkSpec,
        NChunkPools::TChunkStripeKey key,
        int tableIndex) = 0;

    virtual TDataFlowGraph* GetDataFlowGraph() = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITaskHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
