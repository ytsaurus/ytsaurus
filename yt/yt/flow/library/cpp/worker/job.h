#pragma once

#include "buffer_state_manager.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobContext
    : public TRefCounted
{
    std::string WorkerAddress;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    NQueryClient::IColumnEvaluatorCachePtr EvaluatorCache;
    IPayloadConverterCachePtr ConverterCache;
    IMessageDistributorPtr MessageDistributor;
    IResourceManagerPtr ResourceManager;
    IInvokerPtr ControlSerializedInvoker;
    IInvokerPtr SerializedInvoker;
    IInvokerPtr PoolInvoker;
    NObjectClient::TCellTag ClockClusterTag;
    IUniqueSeqNoProviderPtr UniqueSeqNoProvider;
    TLoadThroughputThrottlerPtr LoadThroughputThrottler;
    TComputationStreamSpecStoragePtr StreamSpecStorage;
    TJobStateCachePtr JobStateCache;
    IExternalPerformanceMetricsReporterPtr ExternalMetricsReporter;
    NYT::NHttp::IClientPtr HttpClient;
    NYT::NHttp::IClientPtr HttpsClient;
    NYT::NConcurrency::IPollerPtr Poller;

    //! Raw channel provider to the controller's distributed-throttler service.
    //! Forwarded into TComputationContext.
    std::function<NRpc::IChannelPtr()> DistributedThrottlerControllerChannelProvider;
};

DEFINE_REFCOUNTED_TYPE(TJobContext);

////////////////////////////////////////////////////////////////////////////////

struct TJobOrchidState
    : public virtual NYTree::TYsonStruct
{
    TJobStatusPtr Status;
    TComputationOrchidStatePtr Computation;

    REGISTER_YSON_STRUCT(TJobOrchidState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobOrchidState);

////////////////////////////////////////////////////////////////////////////////

struct IJob
    : public TRefCounted
{
    virtual TFuture<void> Start() = 0;
    virtual TFuture<void> Reconfigure(i64 specGeneration, TDynamicJobSpecPtr dynamicSpec) = 0;
    virtual TFuture<void> UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;
    virtual TFuture<void> UpdateTraverseData(TToPartitionTraverseDataPtr traverse) = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;
    virtual TFuture<void> Stop() = 0;
    //! Stops the job so that its status reports |error| as the failure reason.
    virtual TFuture<void> Stop(TError error) = 0;

    virtual TJobId GetJobId() = 0;
    virtual TComputationId GetComputationId() = 0;
    virtual TFuture<TJobStatusPtr> GetStatus() = 0;
    virtual IInputBufferPtr GetInputBuffer() = 0;

    virtual TFuture<TJobOrchidStatePtr> GetOrchidState() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJob);

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateJob(
    TJobContextPtr jobContext,
    TJobStreamLimitUsageStates streamLimitUsageStates,
    i64 specGeneration,
    TJobSpecPtr jobSpec,
    TDynamicJobSpecPtr dynamicJobSpec,
    TToPartitionTraverseDataPtr traverseData,
    TWatermarkStatePtr watermarkState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
