#include "stdafx.h"
#include "garbage_collector.h"
#include "private.h"
#include "config.h"
#include "object_manager.h"

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

    LOG_DEBUG("Object has become zombie (ObjectId: %v)",
        object->GetId());
    YCHECK(Zombies_.insert(object).second);
}

void TGarbageCollector::DestroyZombie(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(Zombies_.erase(object) == 1);

    auto objectManager = Bootstrap_->GetObjectManager();
    auto handler = objectManager->GetHandler(object->GetType());
    handler->Destroy(object);

    if (object->GetObjectWeakRefCounter() > 0) {
        LOG_DEBUG("Zombie has become ghost (ObjectId: %v, WeakRefCounter: %v)",
            object->GetId(),
            object->GetObjectWeakRefCounter());
        YCHECK(Ghosts_.insert(object).second);
        object->SetDestroyed();
    } else {
        LOG_DEBUG("Zombie disposed (ObjectId: %v)",
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
        LOG_DEBUG("Ghost disposed (ObjectId: %v)",
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
        auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        LOG_DEBUG_UNLESS(hydraManager->IsRecovery(), "Zombie queue is empty");
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

    LOG_DEBUG("Starting sweep for %v zombie objects",
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

int TGarbageCollector::GetZombieCount() const
{
    return static_cast<int>(Zombies_.size());
}

int TGarbageCollector::GetGhostCount() const
{
    return static_cast<int>(Ghosts_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
