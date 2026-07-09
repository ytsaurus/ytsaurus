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

} // namespace NYT::NFlow::NController
