#include "stdafx.h"
#include "garbage_collector.h"
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
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
}

void TGarbageCollector::Start()
{
    YCHECK(!SweepExecutor_);
    SweepExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)),
        Config_->GCSweepPeriod,
        EPeriodicExecutorMode::Manual);
    SweepExecutor_->Start();
}

void TGarbageCollector::Stop()
{
    if (SweepExecutor_) {
        SweepExecutor_->Stop();
        SweepExecutor_.Reset();
    }
}

void TGarbageCollector::Save(NCellMaster::TSaveContext& context) const
{
    yhash_set<TObjectBase*> allZombies;
    for (auto* object : Zombies_) {
        YCHECK(allZombies.insert(object).second);
    }
    for (auto* object : LockedZombies_) {
        YCHECK(allZombies.insert(object).second);
    }
    NYT::Save(context, allZombies);
}

void TGarbageCollector::Load(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NYT::Load(context, Zombies_);
    LockedZombies_.clear();

    CollectPromise_ = NewPromise<void>();
    if (Zombies_.empty()) {
        CollectPromise_.Set();
    }
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies_.clear();
    LockedZombies_.clear();

    CollectPromise_ = NewPromise<void>();
    CollectPromise_.Set();
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CollectPromise_;
}

bool TGarbageCollector::IsEnqueued(TObjectBase* object) const
{
    return Zombies_.find(object) != Zombies_.end() ||
           LockedZombies_.find(object) != LockedZombies_.end();
}

void TGarbageCollector::Enqueue(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());

    if (Zombies_.empty() && LockedZombies_.empty() && CollectPromise_.IsSet()) {
        CollectPromise_ = NewPromise<void>();
    }

    if (object->IsLocked()) {
        YCHECK(LockedZombies_.insert(object).second);
        LOG_DEBUG("Object is put into locked zombie queue (ObjectId: %v)",
            object->GetId());
    } else {
        YCHECK(Zombies_.insert(object).second);
        LOG_TRACE("Object is put into zombie queue (ObjectId: %v)",
            object->GetId());
    }
}

void TGarbageCollector::Unlock(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());
    YASSERT(!object->IsLocked());

    YCHECK(LockedZombies_.erase(object) == 1);
    YCHECK(Zombies_.insert(object).second);
    
    LOG_DEBUG("Object is unlocked and moved to zombie queue (ObjectId: %v)",
        object->GetId());
}

void TGarbageCollector::UnlockAll()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (auto* object : LockedZombies_) {
        YASSERT(object->IsLocked());
        YCHECK(Zombies_.insert(object).second);
    }
    LockedZombies_.clear();
}

void TGarbageCollector::Dequeue(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(Zombies_.erase(object) == 1);
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (Zombies_.empty() && LockedZombies_.empty()) {
        auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        LOG_DEBUG_UNLESS(hydraManager->IsRecovery(), "GC queue is empty");
        CollectPromise_.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(&Zombies_);

    auto hydraFacade = Bootstrap_->GetHydraFacade();
    auto hydraManager = hydraFacade->GetHydraManager();
    if (Zombies_.empty() || !hydraManager->IsActiveLeader()) {
        SweepExecutor_->ScheduleNext();
        return;
    }

    // Extract up to MaxObjectsPerGCSweep objects and post a mutation.
    NProto::TReqDestroyObjects request;
    for (auto it = Zombies_.begin();
         it != Zombies_.end() && request.object_ids_size() < Config_->MaxObjectsPerGCSweep;
         ++it)
    {
        auto* object = *it;
        ToProto(request.add_object_ids(), object->GetId());
    }

    LOG_DEBUG("Starting GC sweep for %v objects",
        request.object_ids_size());

    auto this_ = MakeStrong(this);
    auto invoker = hydraFacade->GetEpochAutomatonInvoker();
    Bootstrap_
        ->GetObjectManager()
        ->CreateDestroyObjectsMutation(request)
        ->Commit()
        .Subscribe(BIND([this, this_] (const TErrorOr<TMutationResponse>& error) {
            if (error.IsOK()) {
                SweepExecutor_->ScheduleOutOfBand();
            }
            SweepExecutor_->ScheduleNext();
        }).Via(invoker));
}

int TGarbageCollector::GetGCQueueSize() const
{
    return static_cast<int>(Zombies_.size());
}

int TGarbageCollector::GetLockedGCQueueSize() const
{
    return static_cast<int>(LockedZombies_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
