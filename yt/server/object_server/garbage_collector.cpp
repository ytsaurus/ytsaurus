#include "garbage_collector.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/object_server/object_manager.pb.h>

#include <yt/core/misc/collection_helpers.h>

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
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GC),
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)),
        Config_->GCSweepPeriod);
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
    NYT::Save(context, Zombies_);
}

void TGarbageCollector::Load(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NYT::Load(context, Zombies_);
    YCHECK(Ghosts_.empty());

    CollectPromise_ = NewPromise<void>();
    if (Zombies_.empty()) {
        CollectPromise_.Set();
    }
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies_.clear();

    Reset();

    CollectPromise_ = NewPromise<void>();
    CollectPromise_.Set();
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CollectPromise_;
}

void TGarbageCollector::RegisterZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());

    if (Zombies_.empty() && CollectPromise_.IsSet()) {
        CollectPromise_ = NewPromise<void>();
    }

    LOG_TRACE_UNLESS(IsRecovery(), "Object has become zombie (ObjectId: %v)",
        object->GetId());
    YCHECK(Zombies_.insert(object).second);
}

void TGarbageCollector::UnregisterZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(object->GetObjectRefCounter() == 1);

    if (Zombies_.erase(object) == 1) {
        LOG_DEBUG("Object has been resurrected (ObjectId: %v)",
            object->GetId());
        CheckEmpty();
    }
}

void TGarbageCollector::DestroyZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(Zombies_.erase(object) == 1);

    auto objectManager = Bootstrap_->GetObjectManager();
    const auto& handler = objectManager->GetHandler(object->GetType());
    handler->DestroyObject(object);

    if (object->GetObjectWeakRefCounter() > 0) {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become ghost (ObjectId: %v, WeakRefCounter: %v)",
            object->GetId(),
            object->GetObjectWeakRefCounter());
        YCHECK(Ghosts_.insert(object).second);
        object->SetDestroyed();
    } else {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie disposed (ObjectId: %v)",
            object->GetId(),
            object->GetObjectWeakRefCounter());
        delete object;
    }
}

void TGarbageCollector::DisposeGhost(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(!object->IsAlive());
    YASSERT(!object->IsLocked());

    if (object->IsDestroyed()) {
        LOG_TRACE("Ghost disposed (ObjectId: %v)",
            object->GetId());
        YCHECK(Ghosts_.erase(object) == 1);
        delete object;
    }
}

void TGarbageCollector::Reset()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (auto* object : Ghosts_) {
        delete object;
    }
    Ghosts_.clear();
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (Zombies_.empty()) {
        LOG_DEBUG_UNLESS(IsRecovery(), "Zombie queue is empty");
        CollectPromise_.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(&Zombies_);
    ShrinkHashTable(&Ghosts_);
    
    auto hydraFacade = Bootstrap_->GetHydraFacade();
    auto hydraManager = hydraFacade->GetHydraManager();
    if (Zombies_.empty() || !hydraManager->IsActiveLeader()) {
        return;
    }

    // Extract up to MaxWeightPerGCSweep and post a mutation.
    int totalWeight = 0;
    NProto::TReqDestroyObjects request;
    for (const auto* object : Zombies_) {
        ToProto(request.add_object_ids(), object->GetId());
        totalWeight += object->GetGCWeight();
        if (totalWeight >= Config_->MaxWeightPerGCSweep)
            break;
    }

    LOG_DEBUG("Starting zombie objects sweep (Count: %v, Weight: %v)",
        request.object_ids_size(),
        totalWeight);

    auto asyncResult = Bootstrap_
        ->GetObjectManager()
        ->CreateDestroyObjectsMutation(request)
        ->CommitAndLog(Logger);
    WaitFor(asyncResult);
}

int TGarbageCollector::GetZombieCount() const
{
    return static_cast<int>(Zombies_.size());
}

int TGarbageCollector::GetGhostCount() const
{
    return static_cast<int>(Ghosts_.size());
}

bool TGarbageCollector::IsRecovery()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
