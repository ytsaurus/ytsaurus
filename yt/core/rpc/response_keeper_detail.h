#pragma once

#include "response_keeper.h"

#include <core/profiling/profiler.h>

#include <queue>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeperBase
{
protected:
    TResponseKeeperBase(
        TResponseKeeperConfigPtr config,
        const NProfiling::TProfiler& profiler);

    TFuture<TSharedRefArray> TryBeginRequest(
        const TMutationId& id);

    void EndRequest(
        const TMutationId& id,
        TSharedRefArray response,
        TInstant now,
        bool remember);

    void EvictExpiredResponses(TInstant now);

    void Clear();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TResponseKeeperConfigPtr Config_;

    typedef yhash_map<TMutationId, TSharedRefArray> TFinishedResponseMap;
    TFinishedResponseMap FinishedResponses_;

    volatile int FinishedResponseCount_ = 0;
    volatile i64 FinishedResponseSpace_ = 0;

    struct TItem
    {
        TFinishedResponseMap::iterator Iterator;
        TInstant When;
    };

    typedef std::deque<TItem> TResponseQueue;
    TResponseQueue ResponseEvictionQueue_;

    yhash_map<TMutationId, TPromise<TSharedRefArray>> PendingResponses_;

    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter CountCounter_;
    NProfiling::TAggregateCounter SpaceCounter_;

    TSpinLock SpinLock_;

    void UpdateCounters(const TSharedRefArray& data, int delta);
    void OnProfiling();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
