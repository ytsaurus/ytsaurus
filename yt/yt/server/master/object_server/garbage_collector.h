#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>

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

    int EphemeralRefObject(TObject* object);

    /*
     * \note Thread affinity: Automaton or LocalRead
     * May postpone object unreferencing unless in automaton thread.
     */
    void EphemeralUnrefObject(TObject* object);

    /*
     * \note Thread affinity: any
     * Always postpones object unreferencing.
     */
    void EphemeralUnrefObject(TObject* object, TEpoch epoch);

    int WeakRefObject(TObject* object);
    int WeakUnrefObject(TObject* object);

    void RegisterZombie(TObject* object);
    void UnregisterZombie(TObject* object);
    void DestroyZombie(TObject* object);
    const THashSet<TObject*>& GetZombies() const;

    void RegisterRemovalAwaitingCellsSyncObject(TObject* object);
    void UnregisterRemovalAwaitingCellsSyncObject(TObject* object);
    const THashSet<TObject*>& GetRemovalAwaitingCellsSyncObjects() const;

    TObject* GetWeakGhostObject(TObjectId id);

    void Reset();

    void CheckEmpty();

    int GetZombieCount() const;
    int GetEphemeralGhostCount() const;
    int GetEphemeralGhostUnrefQueueSize() const;
    int GetWeakGhostCount() const;
    int GetLockedCount() const;

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TGarbageCollector::OnDynamicConfigChanged, MakeWeak(this));

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
    std::atomic<int> LockedObjectCount_ = 0;

    //! Objects in |RemovalAwaitingCellsSync| life stage.
    THashSet<TObject*> RemovalAwaitingCellsSyncObjects_;

    //! List of the ephemeral ghosts waiting for ephemeral unref.
    TMpscStack<std::pair<TObject*, TEpoch>> EphemeralGhostUnrefQueue_;
    std::atomic<int> EphemeralGhostUnrefQueueSize_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void ClearWeakGhosts();
    void ClearEphemeralGhosts();

    void OnSweep();
    void SweepZombies();
    void SweepEphemeralGhosts();

    void OnObjectRemovalCellsSync();
    bool IsRecovery();

    const TDynamicObjectManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
};

DEFINE_REFCOUNTED_TYPE(TGarbageCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
