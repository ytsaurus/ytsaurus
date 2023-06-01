#include "garbage_collector.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

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
        BIND(&TGarbageCollector::OnSweep, MakeWeak(this)),
        GetDynamicConfig()->GCSweepPeriod);
    SweepExecutor_->Start();

    YT_VERIFY(!ObjectRemovalCellsSyncExecutor_);
    ObjectRemovalCellsSyncExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GarbageCollector),
        BIND(&TGarbageCollector::OnObjectRemovalCellsSync, MakeWeak(this)),
        GetDynamicConfig()->ObjectRemovalCellsSyncPeriod);
    ObjectRemovalCellsSyncExecutor_->Start();

    CollectPromise_ = NewPromise<void>();
    if (Zombies_.empty()) {
        CollectPromise_.Set();
    }

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
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
            objectHolder->SetGhost();

            YT_VERIFY(WeakGhosts_.emplace(objectId, objectHolder.release()).second);
        }
    }
}

void TGarbageCollector::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    using NYT::Load;

    Load(context, Zombies_);
    Load(context, RemovalAwaitingCellsSyncObjects_);

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

int TGarbageCollector::EphemeralRefObject(TObject* object)
{
    Bootstrap_->VerifyPersistentStateRead();

    YT_ASSERT(IsObjectAlive(object));
    YT_ASSERT(object->IsTrunk());

    int ephemeralRefCounter = object->EphemeralRefObject();
    if (ephemeralRefCounter == 1 && object->GetObjectWeakRefCounter() == 0) {
        ++LockedObjectCount_;
    }
    return ephemeralRefCounter;
}

void TGarbageCollector::EphemeralUnrefObject(TObject* object)
{
    Bootstrap_->VerifyPersistentStateRead();

    YT_ASSERT(object->IsTrunk());

    bool shouldDestroyObject =
        object->IsGhost() &&
        object->GetObjectEphemeralRefCounter() == 1 &&
        object->GetObjectWeakRefCounter() == 0;

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    if (hydraFacade->IsAutomatonLocked() && shouldDestroyObject) {
        // Object cannot be destroyed out of automaton thread, so we postpone
        // unreference.
        EphemeralUnrefObject(object, GetCurrentEpoch());
        return;
    }

    if (object->EphemeralUnrefObject() == 0 && object->GetObjectWeakRefCounter() == 0) {
        --LockedObjectCount_;

        if (object->IsGhost()) {
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            YT_VERIFY(!IsObjectAlive(object));

            YT_LOG_TRACE("Ephemeral ghost disposed (ObjectId: %v)",
                object->GetId());
            YT_VERIFY(EphemeralGhosts_.erase(object) == 1);
            delete object;
        }
    }
}

void TGarbageCollector::EphemeralUnrefObject(TObject* object, TEpoch epoch)
{
    VERIFY_THREAD_AFFINITY_ANY();

    EphemeralGhostUnrefQueue_.Enqueue(std::make_pair(object, epoch));
    ++EphemeralGhostUnrefQueueSize_;
}

int TGarbageCollector::WeakRefObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(IsObjectAlive(object));
    YT_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakRefObject();
    if (weakRefCounter == 1 && object->GetObjectEphemeralRefCounter() == 0) {
        ++LockedObjectCount_;
    }
    return weakRefCounter;
}

int TGarbageCollector::WeakUnrefObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsTrunk());

    int weakRefCounter = object->WeakUnrefObject();
    if (weakRefCounter == 0) {
        auto ephemeralRefCounter = object->GetObjectEphemeralRefCounter();
        if (ephemeralRefCounter == 0) {
            --LockedObjectCount_;
        }

        if (object->IsGhost()) {
            YT_VERIFY(!IsObjectAlive(object));

            if (ephemeralRefCounter == 0) {
                YT_LOG_TRACE("Weak ghost disposed (ObjectId: %v)",
                    object->GetId());
                YT_VERIFY(WeakGhosts_.erase(object->GetId()) == 1);
                delete object;
            } else {
                YT_LOG_TRACE("Weak ghost became ephemeral ghost (ObjectId: %v)",
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
    YT_ASSERT(!IsObjectAlive(object));

    if (Zombies_.empty() && CollectPromise_ && CollectPromise_.IsSet()) {
        CollectPromise_ = NewPromise<void>();
    }

    YT_LOG_TRACE("Object has become zombie (ObjectId: %v)",
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

    auto ephemeralRefCounter = object->GetObjectEphemeralRefCounter();
    auto weakRefCounter = object->GetObjectWeakRefCounter();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& handler = objectManager->GetHandler(object);
    handler->DestroyObject(object);

    if (ephemeralRefCounter > 0 || weakRefCounter > 0) {
        handler->RecreateObjectAsGhost(object);
    }

    if (weakRefCounter > 0) {
        YT_LOG_TRACE("Zombie has become weak ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YT_VERIFY(WeakGhosts_.emplace(object->GetId(), object).second);
    } else if (ephemeralRefCounter > 0) {
        YT_ASSERT(weakRefCounter == 0);
        YT_LOG_TRACE("Zombie has become ephemeral ghost (ObjectId: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
            object->GetId(),
            ephemeralRefCounter,
            weakRefCounter);
        YT_VERIFY(EphemeralGhosts_.insert(object).second);
    } else {
        YT_LOG_TRACE("Zombie disposed (ObjectId: %v)",
            object->GetId());
        delete object;
    }
}

const THashSet<TObject*>& TGarbageCollector::GetZombies() const
{
    Bootstrap_->VerifyPersistentStateRead();

    return Zombies_;
}

void TGarbageCollector::RegisterRemovalAwaitingCellsSyncObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(RemovalAwaitingCellsSyncObjects_.insert(object).second);

    YT_LOG_DEBUG("Removal awaiting cells sync object registered (ObjectId: %v)",
        object->GetId());
}

void TGarbageCollector::UnregisterRemovalAwaitingCellsSyncObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (RemovalAwaitingCellsSyncObjects_.erase(object) == 1) {
        YT_LOG_DEBUG("Removal awaiting cells sync object unregistered (ObjectId: %v)",
            object->GetId());
    } else {
        YT_LOG_ALERT("Attempt to unregister an unknown removal awaiting cells sync object (ObjectId: %v)",
            object->GetId());
    }
}

const THashSet<TObject*>& TGarbageCollector::GetRemovalAwaitingCellsSyncObjects() const
{
    Bootstrap_->VerifyPersistentStateRead();

    return RemovalAwaitingCellsSyncObjects_;
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
        YT_ASSERT(object->IsGhost());
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

    for (auto [objectId, object] : WeakGhosts_) {
        YT_VERIFY(object->IsGhost());
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
        YT_LOG_DEBUG("Zombie queue is empty");
        CollectPromise_.Set();
    }
}

void TGarbageCollector::OnSweep()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ShrinkHashTable(Zombies_);
    ShrinkHashTable(EphemeralGhosts_);
    ShrinkHashTable(WeakGhosts_);

    SweepZombies();
    SweepEphemeralGhosts();
}

void TGarbageCollector::SweepZombies()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraFacade = Bootstrap_->GetHydraFacade();
    const auto& hydraManager = hydraFacade->GetHydraManager();
    if (Zombies_.empty() || !hydraManager->IsActiveLeader()) {
        return;
    }

    // Extract up to MaxWeightPerGCSweep and post a mutation.
    int totalWeight = 0;
    std::vector<TObjectId> objectIds;
    for (const auto* object : Zombies_) {
        objectIds.push_back(object->GetId());
        totalWeight += object->GetGCWeight();
        if (totalWeight >= GetDynamicConfig()->MaxWeightPerGCSweep) {
            break;
        }
    }

    YT_LOG_DEBUG("Starting zombie objects sweep (Count: %v, Weight: %v)",
        objectIds.size(),
        totalWeight);

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto result = WaitFor(objectManager->DestroyObjects(std::move(objectIds)));
    YT_LOG_DEBUG_UNLESS(
        result.IsOK(),
        result,
        "Error destroying objects");
}

void TGarbageCollector::SweepEphemeralGhosts()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // TODO(gritukan): Extract ghosts one by one.
    auto objectsToUnref = EphemeralGhostUnrefQueue_.DequeueAll();
    EphemeralGhostUnrefQueueSize_ -= std::ssize(objectsToUnref);
    for (auto [object, epoch] : objectsToUnref) {
        if (epoch == GetCurrentEpoch()) {
            EphemeralUnrefObject(object);
        }
    }
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

    auto result = WaitFor(AllSucceeded(futures));
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
    return std::ssize(Zombies_);
}

int TGarbageCollector::GetEphemeralGhostCount() const
{
    return std::ssize(EphemeralGhosts_);
}

int TGarbageCollector::GetEphemeralGhostUnrefQueueSize() const
{
    return EphemeralGhostUnrefQueueSize_;
}

int TGarbageCollector::GetWeakGhostCount() const
{
    return std::ssize(WeakGhosts_);
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

void TGarbageCollector::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    SweepExecutor_->SetPeriod(GetDynamicConfig()->GCSweepPeriod);
    ObjectRemovalCellsSyncExecutor_->SetPeriod(GetDynamicConfig()->ObjectRemovalCellsSyncPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
