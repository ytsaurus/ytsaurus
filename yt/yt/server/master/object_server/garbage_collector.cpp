#include "garbage_collector.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/ytlib/api/native/connection.h>

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

TGarbageCollector::TGarbageCollector(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    YT_VERIFY(Bootstrap_);
}

void TGarbageCollector::Start()
{
    YT_VERIFY(!SweepExecutor_);
    SweepExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GarbageCollector),
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)));
    SweepExecutor_->Start();

    YT_VERIFY(!ObjectRemovalCellsSyncExecutor_);
    ObjectRemovalCellsSyncExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GarbageCollector),
        BIND(&TGarbageCollector::OnObjectRemovalCellsSync, MakeWeak(this)));
    ObjectRemovalCellsSyncExecutor_->Start();

    CollectPromise_ = NewPromise<void>();
    if (Zombies_.empty()) {
        CollectPromise_.Set();
    }

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    OnDynamicConfigChanged();
}

void TGarbageCollector::Stop()
{
    if (SweepExecutor_) {
        SweepExecutor_->Stop();
        SweepExecutor_.Reset();
    }

    if (ObjectRemovalCellsSyncExecutor_) {
        ObjectRemovalCellsSyncExecutor_->Stop();
        ObjectRemovalCellsSyncExecutor_.Reset();
    }

    CollectPromise_.Reset();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TGarbageCollector::SaveKeys(NCellMaster::TSaveContext& context) const
{
    // NB: normal THashMap serialization won't do. Weak ghosts are already
    // destroyed. Only TObject part of the objects should be saved.

    TSizeSerializer::Save(context, WeakGhosts_.size());

    auto saveIterators = GetSortedIterators(WeakGhosts_);

    for (auto it : saveIterators) {
        Save(context, it->first);
        // Save only the base object part!
        const auto& object = *static_cast<TObject*>(it->second);
        Save(context, object);
    }
}

void TGarbageCollector::SaveValues(NCellMaster::TSaveContext& context) const
{
    Save(context, Zombies_);
    Save(context, RemovalAwaitingCellsSyncObjects_);
}

void TGarbageCollector::LoadKeys(NCellMaster::TLoadContext& context)
{
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

            YT_VERIFY(WeakGhosts_.emplace(objectId, objectHolder.get()).second);

            objectHolder.release();
        }
    }
}

void TGarbageCollector::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    using NYT::Load;

    Load(context, Zombies_);

    // COMPAT(babenko)
    if (context.GetVersion() >= EMasterReign::SyncCellsBeforeRemoval) {
        Load(context, RemovalAwaitingCellsSyncObjects_);
    }

    YT_VERIFY(EphemeralGhosts_.empty());
}

void TGarbageCollector::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Zombies_.clear();
    RemovalAwaitingCellsSyncObjects_.clear();

    ClearWeakGhosts();

    Reset();

    LockedObjectCount_ = 0;
}

TFuture<void> TGarbageCollector::Collect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(CollectPromise_);
    return CollectPromise_;
}

int TGarbageCollector::EphemeralRefObject(TObject* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!IsRecovery());
    YT_ASSERT(object->IsAlive());
    YT_ASSERT(object->IsTrunk());

    int ephemeralRefCounter = object->EphemeralRefObject(epoch);
    if (ephemeralRefCounter == 1 && object->GetObjectWeakRefCounter() == 0) {
        ++LockedObjectCount_;
    }
    return ephemeralRefCounter;
}

int TGarbageCollector::EphemeralUnrefObject(TObject* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!IsRecovery());
    YT_ASSERT(object->IsTrunk());

    int ephemeralRefCounter = object->EphemeralUnrefObject(epoch);
    if (ephemeralRefCounter == 0 && object->GetObjectWeakRefCounter() == 0) {
        --LockedObjectCount_;

        if (object->IsDestroyed()) {
            YT_VERIFY(!object->IsAlive());

            YT_LOG_TRACE("Ephemeral ghost disposed (ObjectId: %v)",
                object->GetId());
            YT_VERIFY(EphemeralGhosts_.erase(object) == 1);
            delete object;
        }
    }
    return ephemeralRefCounter;
}

int TGarbageCollector::WeakRefObject(TObject* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsAlive());
    YT_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakRefObject();
    if (weakRefCounter == 1 && object->GetObjectEphemeralRefCounter(epoch) == 0) {
        ++LockedObjectCount_;
    }
    return weakRefCounter;
}

int TGarbageCollector::WeakUnrefObject(TObject* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakUnrefObject();
    if (weakRefCounter == 0) {
        auto ephemeralRefCounter = object->GetObjectEphemeralRefCounter(epoch);

        if (ephemeralRefCounter == 0) {
            --LockedObjectCount_;
        }

        if (object->IsDestroyed()) {
            YT_VERIFY(!object->IsAlive());

            if (ephemeralRefCounter == 0) {
                YT_LOG_TRACE_UNLESS(IsRecovery(), "Weak ghost disposed (ObjectId: %v)",
                    object->GetId());
                YT_VERIFY(WeakGhosts_.erase(object->GetId()) == 1);
                delete object;
            } else {
                YT_LOG_TRACE_UNLESS(IsRecovery(), "Weak ghost became ephemeral ghost (ObjectId: %v)",
                    object->GetId());
                YT_VERIFY(WeakGhosts_.erase(object->GetId()) == 1);
                YT_VERIFY(EphemeralGhosts_.insert(object).second);
            }
        }
    }

    return weakRefCounter;
}

void TGarbageCollector::RegisterZombie(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!object->IsAlive());

    if (Zombies_.empty() && CollectPromise_ && CollectPromise_.IsSet()) {
        CollectPromise_ = NewPromise<void>();
    }

    YT_LOG_TRACE_UNLESS(IsRecovery(), "Object has become zombie (ObjectId: %v)",
        object->GetId());
    YT_VERIFY(Zombies_.insert(object).second);
}

void TGarbageCollector::UnregisterZombie(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->GetObjectRefCounter() == 1);

    if (Zombies_.erase(object) == 1) {
        YT_LOG_DEBUG("Object has been resurrected (ObjectId: %v)",
            object->GetId());
        CheckEmpty();
    }
}

void TGarbageCollector::DestroyZombie(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(Zombies_.erase(object) == 1);

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto ephemeralRefCounter = objectManager->GetObjectEphemeralRefCounter(object);
    auto weakRefCounter = objectManager->GetObjectWeakRefCounter(object);

    const auto& handler = objectManager->GetHandler(object->GetType());
    handler->DestroyObject(object);

    if (weakRefCounter > 0) {
        YT_LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become weak ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YT_VERIFY(WeakGhosts_.emplace(object->GetId(), object).second);
        object->SetDestroyed();
    } else if (ephemeralRefCounter > 0) {
        YT_ASSERT(weakRefCounter == 0);
        YT_LOG_TRACE_UNLESS(IsRecovery(), "Zombie has become ephemeral ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YT_VERIFY(EphemeralGhosts_.insert(object).second);
        object->SetDestroyed();
    } else {
        YT_LOG_TRACE_UNLESS(IsRecovery(), "Zombie disposed (ObjectId: %v)",
            object->GetId());
        delete object;
    }
}

void TGarbageCollector::RegisterRemovalAwaitingCellsSyncObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(RemovalAwaitingCellsSyncObjects_.insert(object).second);

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removal awaiting cells sync object registered (ObjectId: %v)",
        object->GetId());
}

void TGarbageCollector::UnregisterRemovalAwaitingCellsSyncObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (RemovalAwaitingCellsSyncObjects_.erase(object) == 1) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removal awaiting cells sync object unregistered (ObjectId: %v)",
            object->GetId());
    } else {
        YT_LOG_ALERT_UNLESS(IsRecovery(), "Attempt to unregister an unknown removal awaiting cells sync object (ObjectId: %v)",
            object->GetId());
    }
}

TObject* TGarbageCollector::GetWeakGhostObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GetOrCrash(WeakGhosts_, id);
}

void TGarbageCollector::Reset()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ClearEphemeralGhosts();
}

void TGarbageCollector::ClearEphemeralGhosts()
{
    YT_LOG_INFO("Started deleting ephemeral ghost objects (Count: %v)",
        EphemeralGhosts_.size());

    for (auto* object : EphemeralGhosts_) {
        YT_ASSERT(object->IsDestroyed());
        delete object;
    }

    LockedObjectCount_ -= EphemeralGhosts_.size();

    EphemeralGhosts_.clear();
    YT_LOG_INFO("Finished deleting ephemeral ghost objects");
}

void TGarbageCollector::ClearWeakGhosts()
{
    YT_LOG_INFO("Started deleting weak ghost objects (Count: %v)",
        WeakGhosts_.size());

    for (const auto& pair : WeakGhosts_) {
        auto* object = pair.second;
        YT_VERIFY(object->IsDestroyed());
        delete object;
    }

    LockedObjectCount_ -= WeakGhosts_.size();

    WeakGhosts_.clear();
    YT_LOG_INFO("Finished deleting weak ghost objects");
}

void TGarbageCollector::CheckEmpty()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (Zombies_.empty() && CollectPromise_ && !CollectPromise_.IsSet()) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Zombie queue is empty");
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
        if (totalWeight >= GetDynamicConfig()->MaxWeightPerGCSweep) {
            break;
        }
    }

    YT_LOG_DEBUG("Starting zombie objects sweep (Count: %v, Weight: %v)",
        request.object_ids_size(),
        totalWeight);

    auto asyncResult = Bootstrap_
        ->GetObjectManager()
        ->CreateDestroyObjectsMutation(request)
        ->CommitAndLog(Logger);
    Y_UNUSED(WaitFor(asyncResult));
}

void TGarbageCollector::OnObjectRemovalCellsSync()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    const auto& hydraManager = hydraFacade->GetHydraManager();
    if (RemovalAwaitingCellsSyncObjects_.empty() || !hydraManager->IsActiveLeader()) {
        return;
    }

    std::vector<TObjectId> objectIds;
    objectIds.reserve(RemovalAwaitingCellsSyncObjects_.size());
    for (auto* object : RemovalAwaitingCellsSyncObjects_) {
        objectIds.push_back(object->GetId());
    }

    std::vector<TCellId> secondaryCellIds;
    const auto& connection = Bootstrap_->GetClusterConnection();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
        secondaryCellIds.push_back(connection->GetMasterCellId(cellTag));
    }

    std::vector<TFuture<void>> futures;
    for (auto cellId : secondaryCellIds) {
        futures.push_back(connection->SyncHiveCellWithOthers(secondaryCellIds, cellId));
    }

    auto result = WaitFor(Combine(futures));
    if (!result.IsOK()) {
        YT_LOG_WARNING(result, "Error synchronizing secondary cells");
        return;
    }

    NProto::TReqConfirmRemovalAwaitingCellsSyncObjects request;
    ToProto(request.mutable_object_ids(), objectIds);

    YT_LOG_DEBUG("Confirming removal awaiting cells sync objects (ObjectIds: %v)",
        objectIds);

    auto asyncResult = CreateMutation(hydraManager, request)
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

const TDynamicObjectManagerConfigPtr& TGarbageCollector::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->ObjectManager;
}

void TGarbageCollector::OnDynamicConfigChanged()
{
    SweepExecutor_->SetPeriod(GetDynamicConfig()->GCSweepPeriod);
    ObjectRemovalCellsSyncExecutor_->SetPeriod(GetDynamicConfig()->ObjectRemovalCellsSyncPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
