#pragma once

#include <yt/yt/flow/library/cpp/common/controller/public.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWorkerState,
    // Handshake has been just received, waiting for the first heartbeat.
    (WaitingForInitialHeartbeat)
    // Handshake and at least one heartbeat have been received.
    (Registered)
    // Worker is registered but connection is faulty.
    (RegisteredFaulty)
    // This worker has been unregistered and its instance is no longer valid.
    (Unregistered)
);

// XXX(babenko): revise
DEFINE_ENUM(EControlQueue,
    (Default)
    (YTConnector)
    (WorkerTracker)
    (StaticOrchid)
    (Admin)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IYTConnector)
DECLARE_REFCOUNTED_STRUCT(IWorkerTracker)
DECLARE_REFCOUNTED_STRUCT(IController)

DECLARE_REFCOUNTED_STRUCT(IPersistedStateManager)
DECLARE_REFCOUNTED_STRUCT(ILeaseManager)

DECLARE_REFCOUNTED_STRUCT(TJobManagerContext)
DECLARE_REFCOUNTED_STRUCT(IJobManager)
DECLARE_REFCOUNTED_STRUCT(IFlowExecutor)
DECLARE_REFCOUNTED_STRUCT(IThrottlerHost)

DECLARE_REFCOUNTED_CLASS(TWorker)

// TODO(mikari): Move to Pipeline Spec.
DECLARE_REFCOUNTED_STRUCT(TControllerConfig)

DECLARE_REFCOUNTED_STRUCT(TPersistedStateManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TLeaseManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TElectionManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerServiceConfig)

using TControlActionQueuePtr = NConcurrency::IEnumIndexedFairShareActionQueuePtr<EControlQueue>;

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT::NFlow::NController
