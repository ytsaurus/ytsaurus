#pragma once

#include "key_visitor_store.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/job_state/public.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/tables/key_states.h>
#include <yt/yt/flow/library/cpp/tables/key_visitor_states.h>
#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Static, computation-lifetime configuration for a TKeyVisitor instance.
struct TKeyVisitorContext
    : public TRefCounted
{
    TComputationId ComputationId;
    TStreamId StreamId;
    TKeyVisitorStreamSpecPtr Spec;
    TKeyRange PartitionRange;
    NTables::IKeyStatesPtr KeyStates;
    TJobStateManagerPtr StateManager;
    NTables::IKeyVisitorStatesPtr KeyVisitorStates;
    ITimeProviderPtr TimeProvider;
    IInvokerPtr SerializedInvoker;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! Dynamic configuration delivered through Reconfigure.
struct TDynamicKeyVisitorContext
    : public TRefCounted
{
    TDynamicKeyVisitorStreamSpecPtr DynamicSpec;
    bool Draining = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Per-second hash-units sweep rate that covers |span| in one |periodSeconds|,
//! clamped so the throttler's `period * rate` stays i64-safe.
double ComputeKeyVisitorSweepRate(double span, double periodSeconds);

////////////////////////////////////////////////////////////////////////////////

//! Per-partition object that visits every key in the computation's internal
//! state space at most once per pass. Coverage is delegated to TKeyVisitorStore;
//! this class owns the throttler, the background fill loop, and the
//! consumer-facing buffer.
class TKeyVisitor
    : public TRefCounted
{
public:
    TKeyVisitor(
        TKeyVisitorContextPtr context,
        TDynamicKeyVisitorContextPtr dynamicContext);

    //! Loads persisted coverage via Store and starts the background fill task.
    //! Must be called once before GetNextBatch.
    TFuture<void> Init();

    //! Updates dynamic settings: throttler rate from `period`/`max_keys_per_period`,
    //! draining flag.
    void Reconfigure(TDynamicKeyVisitorContextPtr dynamicContext);

    //! Returns up to |batchSize| ready visits drained from the internal
    //! background-filled buffer. Synchronous: never yields the calling fiber.
    //! When draining or out of work yields an empty vector.
    std::vector<TVisit> GetNextBatch(i64 batchSize);

    //! Signals that all upstream non-visit streams are completed; the next
    //! pass is started as Final, after which the visitor reports itself empty.
    //! One-way and idempotent.
    void SetUpstreamCompleted();

    //! Single-entry inflight map; Empty becomes true once the Final pass is
    //! fully committed and the buffer is drained.
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const;

    //! Persists the coverage diff produced since the last Sync.
    void Sync(NApi::IDynamicTableTransactionPtr transaction);

    //! Stops the background fill executor so the owner can drop its last
    //! strong reference. Idempotent.
    void Stop();

    //! True while the throttler is scaled up by `CatchupSpeedupMultiplier` to
    //! drain accumulated schedule lag. Intended for tests; production code
    //! observes the throttler rate.
    bool IsInCatchup() const;

private:
    const TKeyVisitorContextPtr Context_;
    TDynamicKeyVisitorContextPtr DynamicContext_;
    const NConcurrency::IReconfigurableThroughputThrottlerPtr Throttler_;
    const NLogging::TLogger Logger;

    const TKeyVisitorStorePtr Store_;
    const IStatusErrorStatePtr ScanCapStallError_;
    //! Holds the most recent background-fill failure (List/seqno).
    const IStatusErrorStatePtr BackgroundFillError_;

    bool UpstreamCompleted_ = false;

    THashSet<std::string> NonVisitorJoinerWarned_;

    //! Catch-up mode toggle. Flips to true when the schedule lag exceeds
    //! `CatchupLagThreshold` and scales the throttler by `CatchupSpeedupMultiplier`.
    //! Mutated only on transitions in DoRunBackgroundFillIteration to avoid
    //! churning Throttler_->Reconfigure on every iteration.
    bool InCatchup_ = false;

    //! One chunk of background-prefetched coverage: the sub-range that the
    //! filler MarkScanned'd, plus the visits whose keys still live in it.
    //! As GetNextBatch drains the chunk we either consume the whole entry
    //! (MarkProcessed the full Range and pop) or split it (MarkProcessed
    //! the taken prefix, shift Range.Lower to the first remaining key).
    struct TBufferEntry
    {
        TKeyRange Range;
        std::deque<TVisit> Visits;
    };

    //! FIFO of background-prefetched chunks. Ordered by emission time, not
    //! by key range.
    std::deque<TBufferEntry> Buffer_;
    //! Sum of Visits.size() across all Buffer_ entries — kept here so
    //! BufferRowLimit checks stay O(1).
    i64 BufferRowCount_ = 0;
    //! Rate counters surfaced through BuildInflight as
    //! InflightMetrics->{NewCountPerSec, ProcessedCountPerSec} so that
    //! standard Solomon dashboards see visit-stream throughput like for
    //! source/timer streams. Emitted = visits pushed into Buffer_;
    //! Consumed = visits handed out via GetNextBatch.
    TSimpleEmaCounter EmittedRate_;
    TSimpleEmaCounter ConsumedRate_;

    //! Worker-side raw counters surfaced as
    //! /key_visitor_streams/{registered,persisted}_count, mirroring
    //! /input_streams /source_streams /timer_streams. Drive the standard
    //! "processed messages rate" / "generated messages rate" widgets.
    const NProfiling::TCounter RegisteredCounter_;
    const NProfiling::TCounter PersistedCounter_;
    //! Pages through the current pass, throttling and pushing chunks into
    //! Buffer_. One callback per tick; ScheduleOutOfBand drives the
    //! "continue immediately" case.
    NConcurrency::TPeriodicExecutorPtr BackgroundFillExecutor_;

    enum class EIterationOutcome
    {
        //! No work right now — wait for the next periodic invocation.
        Idle,
        //! Hit a read pressure point — re-enter immediately.
        ContinueImmediately,
    };

    void RunBackgroundFillIteration();
    EIterationOutcome DoRunBackgroundFillIteration();
    EIterationOutcome DoRunBackgroundFillIterationGuarded();
    NConcurrency::TThroughputThrottlerConfigPtr BuildThrottlerConfig() const;
};

DEFINE_REFCOUNTED_TYPE(TKeyVisitor);
DEFINE_REFCOUNTED_TYPE(TKeyVisitorContext);
DEFINE_REFCOUNTED_TYPE(TDynamicKeyVisitorContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
