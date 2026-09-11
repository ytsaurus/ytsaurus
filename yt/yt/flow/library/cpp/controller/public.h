#pragma once

#include <yt/yt/flow/library/cpp/common/controller/public.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

// These live here (not private.h) so cross-role code such as the runner can reach them.
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, PublicControllerLogger, "PublicFlowController");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ControllerProfiler, "", "yt.flow.controller");

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

// Backend that elects the leader and fences its transactions, and the job leases along with it.
DEFINE_ENUM(EElectionBackend,
    // Exclusive Cypress lock on a node under the pipeline path; its master transaction is the
    // prerequisite of the leader's and of the workers' transactions.
    (Cypress)
    // Leader-lease and per-partition rows in the pipeline tables; fenced transactions validate
    // and rewrite them on commit.
    (Dyntable)
    // Chaos lease recorded in the pipeline's leader election lock table. Meant for a pipeline whose
    // tables are chaos replicated: such a commit accepts no other kind of prerequisite (and,
    // conversely, a plain commit accepts no chaos lease).
    (Chaos)
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
DECLARE_REFCOUNTED_STRUCT(TElectionBackendConfigBase)
DECLARE_REFCOUNTED_STRUCT(TCypressElectionBackendConfig)
DECLARE_REFCOUNTED_STRUCT(TDyntableElectionBackendConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosElectionBackendConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerServiceConfig)

using TControlActionQueuePtr = NConcurrency::IEnumIndexedFairShareActionQueuePtr<EControlQueue>;

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT::NFlow::NController
