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

    void StartSweep();
    void StopSweep();

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
    TObjectManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NConcurrency::TPeriodicExecutorPtr SweepExecutor;

    //! Contains objects with zero ref counter and zero lock counter.
    yhash_set<TObjectBase*> Zombies;

    //! Contains objects with zero ref counter and positive lock counter.
    yhash_set<TObjectBase*> LockedZombies;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> CollectPromise;


    void OnSweep();
    void OnCommitSucceeded();
    void OnCommitFailed(const TError& error);


    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
