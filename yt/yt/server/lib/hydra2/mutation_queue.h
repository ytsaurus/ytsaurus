#pragma once

#include "private.h"
#include "mutation_context.h"
#include "decorated_automaton.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hydra2/hydra_service_proxy.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/invoker_alarm.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/tracing/async_queue_trace.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class TMutationQueue
{
public:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
