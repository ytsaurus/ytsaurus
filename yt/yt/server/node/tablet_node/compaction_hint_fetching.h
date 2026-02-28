#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/lsm/compaction_hints.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TPerCellCompactionHintFetchingContext
{
    const NProfiling::TCounter FinishedRequestCount;
    const NProfiling::TCounter FailedRequestCount;

    const NProfiling::TTimeCounter ParseCumulativeTime;

    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for fetching compaction hints.
/*!
 * Short-lived: pipeline is created when fetch is requested for the store and dies when LSM feedback received.
 * Subclasses override |DoFetch| method which does actual fetching and fills |Payload| with the typed hint.
 * Pipelines are coordinated by |TCompactionHintFetcher| (see below).
 *
 * Thread affinity: Automaton.
 */
class TCompactionHintFetchPipeline
    : public TIntrusiveListItem<TCompactionHintFetchPipeline>
    , public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(NLsm::TStoreCompactionHint::TPayload, Payload);

public:
    explicit TCompactionHintFetchPipeline(TSortedChunkStore* store);

    // Add the pipeline to the fetcher queue. Called externally when the pipeline is created.
    void Enqueue();

    // Start actual fetch. Called by |TCompactionHintFetcher|.
    void Fetch();

protected:
    TSortedChunkStore* Store_;

    virtual NLsm::EStoreCompactionHintKind GetStoreCompactionHintKind() const = 0;

    const TCompactionHintFetcherPtr& GetFetcher() const;

    void ExecuteParse(const std::function<void()>& parser) const;

    void OnStoreHasNoHint();

    void FinishFetch(NLsm::TStoreCompactionHint::TPayload&& payload);

    template <template <class T> class TFutureType, class T, class THandler>
    void SubscribeWithErrorHandling(const TFutureType<T>& future, THandler&& handler);

private:
    virtual void DoFetch() = 0;

    IInvokerPtr GetEpochAutomatonInvoker() const;

    void OnRequestFailed(const TError& error);

    template <class T, class THandler, class TErrorOrTType>
    void WrapWithErrorHandling(THandler&& handler, TErrorOrTType&& errorOrRsp);
};

DEFINE_REFCOUNTED_TYPE(TCompactionHintFetchPipeline);

////////////////////////////////////////////////////////////////////////////////

//! Stores a queue of pending fetch pipelines for compaction hints
//! and executes them in arbitrary order with respect to the throttler.
//! Exists per-cell for every kind of hint.
class TCompactionHintFetcher
    : public TRefCounted
{
public:
    DECLARE_BYREF_RO_PROPERTY(TPerCellCompactionHintFetchingContext, Context);

public:
    TCompactionHintFetcher(
        TTabletCellId cellId,
        NLogging::TLogger logger,
        const NProfiling::TProfiler& profiler,
        TCompactionHintFetcherConfigPtr config);

    void Start(IInvokerPtr epochAutomatonInvoker, TCompactionHintFetcherConfigPtr config);

    void Stop();

    void Reconfigure(const TCompactionHintFetcherConfigPtr& config);

    void EnqueuePipeline(const TCompactionHintFetchPipelinePtr& pipeline);

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TCompactionHintFetcherConfigPtr Config_;
    NConcurrency::TPeriodicExecutorPtr FetchingExecutor_;

    TIntrusiveList<TCompactionHintFetchPipeline> Pipelines_;

    const NProfiling::TProfiler Profiler_;

    const NProfiling::TCounter RequestCount_;
    const NProfiling::TCounter ThrottledRequestCount_;

    const NConcurrency::IReconfigurableThroughputThrottlerPtr RequestThrottler_;

    const TPerCellCompactionHintFetchingContext Context_;

    const NLogging::TLogger& Logger;

    bool IsStopped() const;

    void ExecuteEnqueuedPipelines();
};

DEFINE_REFCOUNTED_TYPE(TCompactionHintFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define COMPACTION_HINT_FETCHING_INL_H_
#include "compaction_hint_fetching-inl.h"
#undef COMPACTION_HINT_FETCHING_INL_H_
