#include "stdafx.h"
#include "gc.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"

#include <core/misc/collection_helpers.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/serialize.h>

#include <server/object_server/object_manager.pb.h>

namespace NYT {
namespace NObjectServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

TGarbageCollector::TGarbageCollector(
    TObjectManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(Config);
    YCHECK(Bootstrap);
}

void TGarbageCollector::StartSweep()
{
    YCHECK(!SweepExecutor);
    SweepExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)),
        Config->GCSweepPeriod,
        EPeriodicExecutorMode::Manual);
    SweepExecutor->Start();
}

void TGarbageCollector::StopSweep()
{
    if (SweepExecutor) {
        SweepExecutor->Stop();
        SweepExecutor.Reset();
    }
}

void TGarbageCollector::Save(NCellMaster::TSaveContext& context) const
{
    yhash_set<TObjectBase*> allZombies;
    for (auto* object : Zombies) {
        YCHECK(allZombies.insert(object).second);
    }
    for (auto* object : LockedZombies) {
        YCHECK(allZombies.insert(object).second);
    }
    NYT::Save(context, allZombies);
}

void TGarbageCollector::Load(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NYT::Load(context, Zombies);
    LockedZombies.clear();

    CollectPromise = NewPromise<void>();
    if (Zombies.empty()) {
        CollectPromise.Set();
    }
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies.clear();
    LockedZombies.clear();

    CollectPromise = NewPromise<void>();
    CollectPromise.Set();
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CollectPromise;
}

bool TGarbageCollector::IsEnqueued(TObjectBase* object) const
{
    return Zombies.find(object) != Zombies.end() ||
           LockedZombies.find(object) != LockedZombies.end();
}

void TGarbageCollector::Enqueue(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());

    if (Zombies.empty() && LockedZombies.empty() && CollectPromise.IsSet()) {
        CollectPromise = NewPromise<void>();
    }

    if (object->IsLocked()) {
        YCHECK(LockedZombies.insert(object).second);
        LOG_DEBUG("Object is put into locked zombie queue (ObjectId: %v)",
            object->GetId());
    } else {
        YCHECK(Zombies.insert(object).second);
        LOG_TRACE("Object is put into zombie queue (ObjectId: %v)",
            object->GetId());
    }
}

void TGarbageCollector::Unlock(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());
    YASSERT(!object->IsLocked());

    YCHECK(LockedZombies.erase(object) == 1);
    YCHECK(Zombies.insert(object).second);
    
    LOG_DEBUG("Object is unlocked and moved to zombie queue (ObjectId: %v)",
        object->GetId());
}

void TGarbageCollector::UnlockAll()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (auto* object : LockedZombies) {
        YASSERT(object->IsLocked());
        YCHECK(Zombies.insert(object).second);
    }
    LockedZombies.clear();
}

void TGarbageCollector::Dequeue(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(Zombies.erase(object) == 1);
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (Zombies.empty() && LockedZombies.empty()) {
        auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();
        LOG_DEBUG_UNLESS(hydraManager->IsRecovery(), "GC queue is empty");
        CollectPromise.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(&Zombies);

    auto hydraFacade = Bootstrap->GetHydraFacade();
    auto hydraManager = hydraFacade->GetHydraManager();
    if (Zombies.empty() || !hydraManager->IsActiveLeader()) {
        SweepExecutor->ScheduleNext();
        return;
    }

    // Extract up to MaxObjectsPerGCSweep objects and post a mutation.
    NProto::TReqDestroyObjects request;
    for (auto it = Zombies.begin();
         it != Zombies.end() && request.object_ids_size() < Config->MaxObjectsPerGCSweep;
         ++it)
    {
        auto* object = *it;
        ToProto(request.add_object_ids(), object->GetId());
    }

    LOG_DEBUG("Starting GC sweep for %v objects",
        request.object_ids_size());

    auto this_ = MakeStrong(this);
    auto invoker = hydraFacade->GetEpochAutomatonInvoker();
    Bootstrap
        ->GetObjectManager()
        ->CreateDestroyObjectsMutation(request)
        ->Commit()
        .Subscribe(BIND([this, this_] (const TErrorOr<TMutationResponse>& error) {
            if (error.IsOK()) {
                SweepExecutor->ScheduleOutOfBand();
            }
            SweepExecutor->ScheduleNext();
        }).Via(invoker));
}

int TGarbageCollector::GetGCQueueSize() const
{
    return static_cast<int>(Zombies.size());
}

int TGarbageCollector::GetLockedGCQueueSize() const
{
    return static_cast<int>(LockedZombies.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
