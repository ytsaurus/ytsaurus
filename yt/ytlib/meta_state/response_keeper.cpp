#include "stdafx.h"
#include "response_keeper.h"
#include "config.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

static NProfiling::TAggregateCounter CountCounter("/kept_response_count");
static NProfiling::TAggregateCounter SpaceCounter("/kept_response_space");

////////////////////////////////////////////////////////////////////////////////

TResponseKeeper::TResponseKeeper(
    TResponseKeeperConfigPtr config,
    IInvokerPtr epochStateInvoker)
    : Config(config)
    , StateInvoker(epochStateInvoker)
{
    YCHECK(Config);
    YCHECK(StateInvoker);

    SweepInvoker = New<TPeriodicInvoker>(
        StateInvoker,
        BIND(&TResponseKeeper::TrySweep, MakeWeak(this)),
        Config->SweepPeriod);
    SweepInvoker->Start();
}

void TResponseKeeper::RegisterResponse(
    const TMutationId& id,
    const TSharedRef& data)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(id != NullMutationId);

    auto pair = ResponseMap.insert(std::make_pair(id, data));
    YASSERT(pair.second);

    TItem item;
    item.Iterator = pair.first;
    item.When = TInstant::Now();
    ResponseQueue.push(item);

    UpdateCounters(data, +1);

    LOG_DEBUG("Mutation response kept (MutationId: %s, Size: %" PRISZT ")",
        ~id.ToString(),
        data ? data.Size() : 0);

    TrySweep();
}

bool TResponseKeeper::FindResponse(const TMutationId& id, TSharedRef* data)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(id != NullMutationId);

    auto it = ResponseMap.find(id);
    if (it == ResponseMap.end()) {
        return false;
    }

    *data = it->second;
    return true;
}

void TResponseKeeper::TrySweep()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto deadline = TInstant::Now() - Config->ExpirationPeriod;
    while (!ResponseQueue.empty()) {
        const auto& item = ResponseQueue.back();
        if (item.When > deadline) {
            break;
        }
        
        LOG_DEBUG("Mutation response swept (MutationId: %s)", ~item.Iterator->first.ToString());

        UpdateCounters(item.Iterator->second, -1);
        ResponseMap.erase(item.Iterator);
        ResponseQueue.pop();
    }
}

void TResponseKeeper::UpdateCounters(const TSharedRef& data, int delta)
{
    Profiler.Increment(CountCounter, delta);
    if (data) {
        Profiler.Increment(SpaceCounter, static_cast<i64>(data.Size()) * delta);
    }
}

void TResponseKeeper::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ResponseMap.clear();
    while (!ResponseQueue.empty()) {
        ResponseQueue.pop();
    }
    
    Profiler.Aggregate(CountCounter, 0);
    Profiler.Aggregate(SpaceCounter, 0);

    LOG_DEBUG("All mutation responses are swept");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
