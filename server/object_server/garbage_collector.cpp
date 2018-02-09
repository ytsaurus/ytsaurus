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
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GarbageCollector),
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)),
        Config_->GCSweepPeriod);
    SweepExecutor_->Start();

    CollectPromise_ = NewPromise<void>();
    if (Zombies_.empty()) {
        CollectPromise_.Set();
    }
}

void TGarbageCollector::Stop()
{
    if (SweepExecutor_) {
        SweepExecutor_->Stop();
        SweepExecutor_.Reset();
    }

    CollectPromise_.Reset();
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
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies_.clear();

    Reset();
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(CollectPromise_);
    return CollectPromise_;
}

int TGarbageCollector::WeakRefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!IsRecovery());
    Y_ASSERT(object->IsAlive());
    Y_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakRefObject(epoch);
    if (weakRefCounter == 1) {
        ++LockedObjectCount_;
    }
    return weakRefCounter;
}

int TGarbageCollector::WeakUnrefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!IsRecovery());
    Y_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakUnrefObject(epoch);
    if (weakRefCounter == 0) {
        --LockedObjectCount_;

        if (!object->IsAlive() && object->IsDestroyed()) {
            LOG_TRACE("Ghost disposed (ObjectId: %v)",
                object->GetId());
            YCHECK(Ghosts_.erase(object) == 1);
            delete object;
        }
    }
    return weakRefCounter;
}

void TGarbageCollector::RegisterZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!object->IsAlive());

    if (Zombies_.empty() && CollectPromise_ && CollectPromise_.IsSet()) {
        CollectPromise_ = NewPromise<void>();
    }

    LOG_TRACE_UNLESS(IsRecovery(), "Object has become zombie (ObjectId: %v)",
        object->GetId());
    YCHECK(Zombies_.insert(object).second);
}

void TGarbageCollector::UnregisterZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(object->GetObjectRefCounter() == 1);

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

    const auto& objectManager = Bootstrap_->GetObjectManager();
    int weakRefCounter = objectManager->GetObjectWeakRefCounter(object);

    const auto& handler = objectManager->GetHandler(object->GetType());
    handler->DestroyObject(object);

    if (weakRefCounter > 0) {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become ghost (ObjectId: %v, WeakRefCounter: %v)",
            object->GetId(),
            weakRefCounter);
        YCHECK(Ghosts_.insert(object).second);
        object->SetDestroyed();
    } else {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie disposed (ObjectId: %v)",
            object->GetId(),
            weakRefCounter);
        delete object;
    }
}

void TGarbageCollector::Reset()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_INFO("Started deleting ghost objects (Count: %v)",
        Ghosts_.size());
    for (auto* object : Ghosts_) {
        Y_ASSERT(object->IsDestroyed());
        delete object;
    }
    Ghosts_.clear();
    LOG_INFO("Finished deleting ghost objects");

    LockedObjectCount_ = 0;
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (CollectPromise_ && Zombies_.empty()) {
        LOG_DEBUG_UNLESS(IsRecovery(), "Zombie queue is empty");
        CollectPromise_.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(&Zombies_);
    ShrinkHashTable(&Ghosts_);
    
    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    const auto& hydraManager = hydraFacade->GetHydraManager();
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
    Y_UNUSED(WaitFor(asyncResult));
}

int TGarbageCollector::GetZombieCount() const
{
    return static_cast<int>(Zombies_.size());
}

int TGarbageCollector::GetGhostCount() const
{
    return static_cast<int>(Ghosts_.size());
}

int TGarbageCollector::GetLockedCount() const
{
    return LockedObjectCount_;
}

bool TGarbageCollector::IsRecovery()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
