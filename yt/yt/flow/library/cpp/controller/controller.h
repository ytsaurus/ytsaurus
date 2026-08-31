#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct IController
    : public TRefCounted
{
    virtual void Initialize() = 0;

    virtual void EnsureIsLeader() const = 0;
    virtual TFlowViewKeeperPtr GetFlowViewKeeper() const = 0;
    virtual TNodeInfoPtr GetNodeInfo() const = 0;
    //! Returns the controller-wide provider; all version updates must use this instance.
    virtual IVersionProviderPtr GetVersionProvider() const = 0;

    virtual void RegisterJobStatus(const TJobId& jobId, TJobStatusPtr status) = 0;

    virtual void RegisterWorkerStatus(TStringBuf workerAddress, TWorkerStatusPtr status) = 0;
};

DEFINE_REFCOUNTED_TYPE(IController);

////////////////////////////////////////////////////////////////////////////////

IControllerPtr CreateController(
    TControllerConfigPtr config,
    TNodeInfoPtr controllerNodeInfo,
    IWorkerTrackerPtr workerTracker,
    IThrottlerHostPtr throttlerHost,
    IInvokerPtr invoker,
    IYTConnectorPtr connector,
    IPersistedStateManagerPtr stateManager,
    IPipelineAuthenticatorPtr authenticator,
    bool ignoreSingletonsDynamicConfig,
    NObjectClient::TCellTag clockClusterTag,
    IStatusProfilerPtr rootStatusProfiler);

////////////////////////////////////////////////////////////////////////////////

//! Synchronizes traverse data structures with the current pipeline specification.
/*!
 * This function ensures that the traverse data in the flow view matches the current
 * pipeline specification by:
 * - Adding new streams that appear in the spec but are missing from traverse data
 * - Removing streams that no longer exist in the spec
 * - Initializing new streams with the current flow view timestamp as the default watermark
 *
 * New streams are initialized with the current flow view timestamp to:
 * - Provide better behavior in monitoring systems
 * - Prevent flapping in traverseData->Streams (see YTFLOW-447)
 */
void SyncTraverseDataWithSpec(const TFlowViewPtr& flowView);

////////////////////////////////////////////////////////////////////////////////

//! How long a leader refrains from managing jobs after publishing its address.
//!
//! Derived from the periods that actually govern how long a worker needs to reconnect: it learns
//! the new address on its next discovery tick and then needs a handshake and a heartbeat. Capped by
//! controller_wait_timeout — waiting past the point where the workers abandon their jobs protects
//! nothing.
TDuration GetLeadershipWarmupTimeout(const TDynamicPipelineSpecPtr& dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

//! Time left before a leader that published its address at |publishTime| may manage jobs, given
//! that it is |now|. Zero once the warm-up has elapsed, when it is disabled, or when the layout
//! holds no jobs at all.
//!
//! Leadership is won and published independently: the connector retries writing the leader address
//! in the background (for up to publish_timeout) while scheduling starts right away. Until that
//! write lands no worker can discover this controller, so its worker map stays empty — and job
//! management reads an empty map as "the whole fleet is gone" and recreates every job. A null
//! |publishTime| therefore means the whole warm-up is still ahead.
TDuration GetLeadershipWarmupLeft(const TFlowViewPtr& flowView, TInstant publishTime, TInstant now);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
