#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>

#include <set>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TGarbageCollector
    : public TRefCounted
{
public:
    explicit TGarbageCollector(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    void Clear();

    TFuture<void> Collect();

    int EphemeralRefObject(TObject* object, TEpoch epoch);
    int EphemeralUnrefObject(TObject* object, TEpoch epoch);

    int WeakRefObject(TObject* object, TEpoch epoch);
    int WeakUnrefObject(TObject* object, TEpoch epoch);

    void RegisterZombie(TObject* object);
    void UnregisterZombie(TObject* object);
    void DestroyZombie(TObject* object);

    void RegisterRemovalAwaitingCellsSyncObject(TObject* object);
    void UnregisterRemovalAwaitingCellsSyncObject(TObject* object);

    TObject* GetWeakGhostObject(TObjectId id);

    void Reset();

    void CheckEmpty();

    int GetZombieCount() const;
    int GetEphemeralGhostCount() const;
    int GetWeakGhostCount() const;
    int GetLockedCount() const;

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TClosure DynamicConfigChangedCallback_ = BIND(&TGarbageCollector::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr SweepExecutor_;
    NConcurrency::TPeriodicExecutorPtr ObjectRemovalCellsSyncExecutor_;

    //! Contains objects with zero ref counter.
    //! These are ready for IObjectTypeHandler::Destroy call.
    THashSet<TObject*> Zombies_;

    //! Contains objects with zero ref counter, zero weak ref counter, and positive ephemeral ref counter.
    //! These were already destroyed (via IObjectTypeHandler::Destroy) and await disposal (via |delete|).
    //! Not persisted.
    THashSet<TObject*> EphemeralGhosts_;

    //! Contains objects with zero ref counter and positive weak ref counter
    //! (ephemeral ref counter may be zero or positive, it doesn't matter).
    //! These were already destroyed (via IObjectTypeHandler::Destroy) and await disposal (via |delete|).
    //! NB: weak ghost objects are actually owned (and persisted) by the garbage collector.
    THashMap<TObjectId, TObject*> WeakGhosts_;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> CollectPromise_;

    //! The total number of locked objects, including ghosts.
    int LockedObjectCount_ = 0;

    //! Objects in |RemovalAwaitingCellsSync| life stage.
    THashSet<TObject*> RemovalAwaitingCellsSyncObjects_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void ClearWeakGhosts();
    void ClearEphemeralGhosts();

    void OnSweep();
    void OnObjectRemovalCellsSync();
    bool IsRecovery();

    const TDynamicObjectManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged();
};

DEFINE_REFCOUNTED_TYPE(TGarbageCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
