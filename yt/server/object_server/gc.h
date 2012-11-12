#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/error.h>

#include <ytlib/actions/future.h>

#include <ytlib/profiling/profiler.h>

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

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);
    void Clear();

    TFuture<void> Collect();

    void Enqueue(const TObjectId& id);
    void Dequeue(const TObjectId& id);

private:
    TObjectManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NProfiling::TAggregateCounter QueueSizeCounter;

    TPeriodicInvokerPtr SweepInvoker;

    //! Contains the ids of object have reached ref counter of 0
    //! but are not destroyed yet.
    yhash_set<TObjectId> ZombieIds;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> CollectPromise;

    void Sweep();
    void OnCommitSucceeded();
    void OnCommitFailed(const TError& error);

    void ProfileQueueSize();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
