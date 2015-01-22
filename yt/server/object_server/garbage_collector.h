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

    bool IsEnqueued(TObjectBase* object) const;

    void Enqueue(TObjectBase* object);

    void Unlock(TObjectBase* object);
    void UnlockAll();

    void Dequeue(TObjectBase* object);
    void CheckEmpty();

    int GetGCQueueSize() const;
    int GetLockedGCQueueSize() const;

private:
    TObjectManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr SweepExecutor_;

    //! Contains objects with zero ref counter and zero lock counter.
    yhash_set<TObjectBase*> Zombies_;

    //! Contains objects with zero ref counter and positive lock counter.
    yhash_set<TObjectBase*> LockedZombies_;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> CollectPromise_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void OnSweep();

};

DEFINE_REFCOUNTED_TYPE(TGarbageCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
