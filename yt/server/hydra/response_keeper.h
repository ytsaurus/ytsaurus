#pragma once

#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/public.h>

#include <queue>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeper
    : public TRefCounted
{
public:
    TResponseKeeper(
        TResponseKeeperConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr epochAutomatonInvoker);

    bool FindResponse(const TMutationId& id, TSharedRef* data);
    void RegisterResponse(const TMutationId& id, const TSharedRef& data);
    void Clear();

private:
    TResponseKeeperConfigPtr Config;
    
    NConcurrency::TPeriodicExecutorPtr SweepExecutor;

    typedef yhash_map<TMutationId, TSharedRef> TResponseMap;
    TResponseMap ResponseMap;

    struct TItem
    {
        TResponseMap::iterator Iterator;
        TInstant When;
    };

    typedef std::queue<TItem> TResponseQueue;
    TResponseQueue ResponseQueue;

    NLog::TTaggedLogger Logger;


    void TrySweep();
    void UpdateCounters(const TSharedRef& data, int delta);

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
