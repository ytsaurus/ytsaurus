#include "garbage_collector.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/object_server/object_manager.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;

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

void TGarbageCollector::SaveKeys(NCellMaster::TSaveContext& context) const
{
    // NB: normal THashMap serialization won't do. Weak ghosts are already
    // destroyed. Only TObjectBase part of the objects should be saved.

    TSizeSerializer::Save(context, WeakGhosts_.size());

    auto saveIterators = GetSortedIterators(WeakGhosts_);

    for (auto it : saveIterators) {
        Save(context, it->first);
        // Save only the base object part!
        const auto& object = *static_cast<TObjectBase*>(it->second);
        Save(context, object);
    }
}

void TGarbageCollector::SaveValues(NCellMaster::TSaveContext& context) const
{
    Save(context, Zombies_);
}

void TGarbageCollector::LoadKeys(NCellMaster::TLoadContext& context)
{
    // COMPAT(shakurov)
    if (context.GetVersion() < 718 ||
        (800 <= context.GetVersion() && context.GetVersion() < 806))
    {
        return;
    }

    auto size = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "map[%v]", size);
    WeakGhosts_.clear();
    WeakGhosts_.reserve(size);

    const auto& objectManager = Bootstrap_->GetObjectManager();

    SERIALIZATION_DUMP_INDENT(context) {
        for (size_t i = 0; i < size; ++i) {
            auto objectId = Load<TObjectId>(context);

            SERIALIZATION_DUMP_WRITE(context, "=>");

            const auto& handler = objectManager->GetHandler(TypeFromId(objectId));
            auto objectHolder = handler->InstantiateObject(objectId);
            SERIALIZATION_DUMP_INDENT(context) {
                Load(context, *objectHolder);
            }
            objectHolder->SetDestroyed();

            YCHECK(WeakGhosts_.emplace(objectId, objectHolder.get()).second);

            objectHolder.release();
        }
    }
}

void TGarbageCollector::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    using NYT::Load;

    Load(context, Zombies_);

    // COMPAT(shakurov)
    if (705 <= context.GetVersion() &&
        (context.GetVersion() < 718 || (800 <= context.GetVersion() && context.GetVersion() < 806)))
    {
        THashSet<TObjectBase*> weakGhosts;
        for (const auto& pair : WeakGhosts_) {
            weakGhosts.insert(pair.second);
        }
        NYT::Load(context, WeakGhosts_);
    }

    YCHECK(EphemeralGhosts_.empty());
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies_.clear();

    ClearWeakGhosts();

    Reset();

    LockedObjectCount_ = 0;
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(CollectPromise_);
    return CollectPromise_;
}

int TGarbageCollector::EphemeralRefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!IsRecovery());
    Y_ASSERT(object->IsAlive());
    Y_ASSERT(object->IsTrunk());

    int ephemeralRefCounter = object->EphemeralRefObject(epoch);
    if (ephemeralRefCounter == 1 && object->GetObjectWeakRefCounter() == 0) {
        ++LockedObjectCount_;
    }
    return ephemeralRefCounter;
}

int TGarbageCollector::EphemeralUnrefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!IsRecovery());
    Y_ASSERT(object->IsTrunk());

    int ephemeralRefCounter = object->EphemeralUnrefObject(epoch);
    if (ephemeralRefCounter == 0 && object->GetObjectWeakRefCounter() == 0) {
        --LockedObjectCount_;

        if (object->IsDestroyed()) {
            YCHECK(!object->IsAlive());

            LOG_TRACE("Ephemeral ghost disposed (ObjectId: %v)",
                object->GetId());
            YCHECK(EphemeralGhosts_.erase(object) == 1);
            delete object;
        }
    }
    return ephemeralRefCounter;
}

int TGarbageCollector::WeakRefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(object->IsAlive());
    Y_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakRefObject();
    if (weakRefCounter == 1 && object->GetObjectEphemeralRefCounter(epoch) == 0) {
        ++LockedObjectCount_;
    }
    return weakRefCounter;
}

int TGarbageCollector::WeakUnrefObject(TObjectBase* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakUnrefObject();
    if (weakRefCounter == 0) {
        auto ephemeralRefCounter = object->GetObjectEphemeralRefCounter(epoch);

        if (ephemeralRefCounter == 0) {
            --LockedObjectCount_;
        }

        if (object->IsDestroyed()) {
            YCHECK(!object->IsAlive());

            if (ephemeralRefCounter == 0) {
                LOG_TRACE_UNLESS(IsRecovery(), "Weak ghost disposed (ObjectId: %v)",
                    object->GetId());
                YCHECK(WeakGhosts_.erase(object->GetId()) == 1);
                delete object;
            } else {
                LOG_TRACE_UNLESS(IsRecovery(), "Weak ghost became ephemeral ghost (ObjectId: %v)",
                    object->GetId());
                YCHECK(WeakGhosts_.erase(object->GetId()) == 1);
                YCHECK(EphemeralGhosts_.insert(object).second);
            }
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
    auto ephemeralRefCounter = objectManager->GetObjectEphemeralRefCounter(object);
    auto weakRefCounter = objectManager->GetObjectWeakRefCounter(object);

    const auto& handler = objectManager->GetHandler(object->GetType());
    handler->DestroyObject(object);

    if (weakRefCounter > 0) {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become weak ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YCHECK(WeakGhosts_.emplace(object->GetId(), object).second);
        object->SetDestroyed();
    } else if (ephemeralRefCounter > 0) {
        Y_ASSERT(weakRefCounter == 0);
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become ephemeral ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YCHECK(EphemeralGhosts_.insert(object).second);
        object->SetDestroyed();
    } else {
        LOG_TRACE_UNLESS(IsRecovery(), "Zombie disposed (ObjectId: %v)",
            object->GetId());
        delete object;
    }
}

TObjectBase* TGarbageCollector::GetWeakGhostObject(const TObjectId& id)
{
    auto it = WeakGhosts_.find(id);
    YCHECK(it != WeakGhosts_.end());
    return it->second;
}

void TGarbageCollector::Reset()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ClearEphemeralGhosts();
}

void TGarbageCollector::ClearEphemeralGhosts()
{
    LOG_INFO("Started deleting ephemeral ghost objects (Count: %v)",
        EphemeralGhosts_.size());
    for (auto* object : EphemeralGhosts_) {
        Y_ASSERT(object->IsDestroyed());
        delete object;
    }

    LockedObjectCount_ -= EphemeralGhosts_.size();

    EphemeralGhosts_.clear();
    LOG_INFO("Finished deleting ephemeral ghost objects");
}

void TGarbageCollector::ClearWeakGhosts()
{
    LOG_INFO("Started deleting weak ghost objects (Count: %v)", WeakGhosts_.size());
    for (const auto& pair : WeakGhosts_) {
        auto* object = pair.second;
        YCHECK(object->IsDestroyed());
        delete object;
    }

    LockedObjectCount_ -= WeakGhosts_.size();

    WeakGhosts_.clear();
    LOG_INFO("Finished deleting weak ghost objects");
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (Zombies_.empty() && CollectPromise_ && !CollectPromise_.IsSet()) {
        LOG_DEBUG_UNLESS(IsRecovery(), "Zombie queue is empty");
        CollectPromise_.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(&Zombies_);
    ShrinkHashTable(&EphemeralGhosts_);
    ShrinkHashTable(&WeakGhosts_);

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

int TGarbageCollector::GetEphemeralGhostCount() const
{
    return static_cast<int>(EphemeralGhosts_.size());
}

int TGarbageCollector::GetWeakGhostCount() const
{
    return static_cast<int>(WeakGhosts_.size());
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

} // namespace NYT::NObjectServer
