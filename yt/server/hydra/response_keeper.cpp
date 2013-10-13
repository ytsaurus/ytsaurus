#include "stdafx.h"
#include "response_keeper.h"
#include "config.h"

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = HydraProfiler;

static NProfiling::TAggregateCounter CountCounter("/kept_response_count");
static NProfiling::TAggregateCounter SpaceCounter("/kept_response_space");

////////////////////////////////////////////////////////////////////////////////

TResponseKeeper::TResponseKeeper(
    TResponseKeeperConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr automatonInvoker)
    : Config(config)
    , Logger(HydraLogger)
{
    YCHECK(Config);
    YCHECK(cellManager);
    YCHECK(automatonInvoker);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(cellManager->GetCellGuid())));

    SweepExecutor = New<TPeriodicExecutor>(
        automatonInvoker,
        BIND(&TResponseKeeper::TrySweep, MakeWeak(this)),
        Config->SweepPeriod);
    SweepExecutor->Start();
}

void TResponseKeeper::RegisterResponse(
    const TMutationId& id,
    const TSharedRef& data)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(id != NullMutationId);

    auto pair = ResponseMap.insert(std::make_pair(id, data));
    YASSERT(pair.second);

    TItem item;
    item.Iterator = pair.first;
    item.When = TInstant::Now();
    ResponseQueue.push(item);

    UpdateCounters(data, +1);

    LOG_DEBUG("Mutation response kept (MutationId: %s, Size: %" PRISZT ")",
        ~ToString(id),
        data ? data.Size() : 0);

    TrySweep();
}

bool TResponseKeeper::FindResponse(const TMutationId& id, TSharedRef* data)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
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
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto deadline = TInstant::Now() - Config->ExpirationPeriod;
    while (!ResponseQueue.empty()) {
        const auto& item = ResponseQueue.front();
        if (item.When > deadline) {
            break;
        }

        LOG_DEBUG("Mutation response swept (MutationId: %s)", ~ToString(item.Iterator->first));

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
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ResponseMap.clear();
    while (!ResponseQueue.empty()) {
        ResponseQueue.pop();
    }

    Profiler.Aggregate(CountCounter, 0);
    Profiler.Aggregate(SpaceCounter, 0);

    LOG_DEBUG("All mutation responses are swept");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
