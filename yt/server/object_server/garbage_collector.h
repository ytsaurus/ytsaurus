#pragma once

#include "public.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/profiling/profiler.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TGarbageCollector
    : public TRefCounted
{
public:
    TGarbageCollector(
        TObjectManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
    void Clear();

    TFuture<void> Collect();

    void RegisterZombie(TObjectBase* object);
    void DestroyZombie(TObjectBase* object);

    void DisposeGhost(TObjectBase* object);

    void Reset();

    void CheckEmpty();

    int GetZombieCount() const;
    int GetGhostCount() const;

private:
    const TObjectManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr SweepExecutor_;

    //! Contains objects with zero ref counter.
    //! These are ready for IObjectTypeHandler::Destroy call.
    yhash_set<TObjectBase*> Zombies_;

    //! Contains objects with zero ref counter and positive weak ref counter.
    //! These were already destroyed (via IObjectTypeHandler::Destroy) and await disposal (via |delete|).
    yhash_set<TObjectBase*> Ghosts_;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> CollectPromise_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void OnSweep();

};

DEFINE_REFCOUNTED_TYPE(TGarbageCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
