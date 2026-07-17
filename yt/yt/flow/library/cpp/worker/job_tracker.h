#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobTrackerContext
    : public TRefCounted
{
    TNodeInfoPtr WorkerNodeInfo;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr ControlInvoker;
    IMessageDistributorPtr MessageDistributor;
    IInputManagerPtr InputManager;
    IPipelineAuthenticatorPtr PipelineAuthenticator;
    TStreamSpecStoragePtr StreamSpecStorage;
    IJobDirectoryPtr JobDirectory;
    NObjectClient::TCellTag ClockClusterTag;
    NYT::NHttp::IClientPtr HttpClient;
    NYT::NHttp::IClientPtr HttpsClient;
    NYT::NConcurrency::IPollerPtr Poller;
    IStatusProfilerPtr StatusProfiler;

    //! Returns the current channel to the controller's distributed-throttler
    //! service, or null if disconnected. Forwarded into each computation's
    //! context. Thread-safe.
    std::function<NRpc::IChannelPtr()> DistributedThrottlerChannel;

    NApi::IClientPtr GetClient() const;
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerContext);

////////////////////////////////////////////////////////////////////////////////

struct IJobTracker
    : public TRefCounted
{
    //! Stops all jobs with |error| but keeps their runtime records, so their statuses keep
    //! reporting the failure to the controller until the jobs are removed from the layout.
    virtual void CancelAllJobs(TError error) = 0;

    virtual TFuture<std::vector<TJobStatusPtr>> GetStatuses() = 0;

    virtual THashMap<TResourceId, TWorkerResourceStatusPtr> GetResourceStatuses() = 0;

    virtual THashMap<TResourceId, EPreloadedResourceState> GetPreloadedStates() = 0;

    virtual void Reconfigure(
        TExecutionSpecPtr executionSpec,
        const THashMap<TJobId, TDynamicPartitionSpecPtr>& dynamicComputationPartitionSpecs) = 0;

    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobTracker);

////////////////////////////////////////////////////////////////////////////////

IJobTrackerPtr CreateJobTracker(TJobTrackerContextPtr context);

////////////////////////////////////////////////////////////////////////////////

//! Computes the size of the job thread pool from the dynamic spec and node info.
int GetJobThreadPoolSize(const TDynamicPipelineSpecPtr& dynamicSpec, const TNodeInfoPtr& nodeInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
