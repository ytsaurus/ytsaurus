#include "object.h"

#include "garbage_collector.h"
#include "object_manager.h"

#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/client/object_client/helpers.h>

#include <util/generic/algorithm.h>

namespace NYT::NObjectServer {

using namespace NObjectClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSequoiaServer;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

NCellMaster::TBootstrap* Bootstrap;

thread_local bool InAutomatonThread;

// This context is shared between automaton thread and local read threads.
thread_local TEpochContextPtr EpochContext;

struct TEpochRefCounterShard
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
};

constexpr int EpochRefCounterShardCount = 256;
static_assert(IsPowerOf2(EpochRefCounterShardCount), "EpochRefCounterShardCount must be a power of 2");

std::array<TEpochRefCounterShard, EpochRefCounterShardCount> EpochRefCounterShards_;

thread_local bool InTeardownFlag;
thread_local int InMutationCounter;

thread_local std::vector<TObject*> ObjectsWithScheduledUnref;
thread_local std::vector<TObject*> ObjectsWithScheduledWeakUnref;

////////////////////////////////////////////////////////////////////////////////

bool IsInAutomatonThread()
{
    return InAutomatonThread;
}

bool IsInTeardown()
{
    return InTeardownFlag;
}

void VerifyAutomatonThreadAffinity()
{
    YT_VERIFY(IsInAutomatonThread());
}

void VerifyPersistentStateRead()
{
    // NB: IsInAutomatonThread is fork-aware while VerifyPersistentStateRead is not.
    if (!IsInAutomatonThread()) {
        Bootstrap->VerifyPersistentStateRead();
    }
}

void DoFlushObjectUnrefs()
{
    const auto& objectManager = Bootstrap->GetObjectManager();
    while (
        !ObjectsWithScheduledUnref.empty() ||
        !ObjectsWithScheduledWeakUnref.empty())
    {
        auto objectsWithScheduledUnref = std::move(ObjectsWithScheduledUnref);
        Sort(objectsWithScheduledUnref, TObjectIdComparer());
        for (auto* object : objectsWithScheduledUnref) {
            objectManager->UnrefObject(object);
        }

        auto objectsWithScheduledWeakUnref = std::move(ObjectsWithScheduledWeakUnref);
        Sort(objectsWithScheduledWeakUnref, TObjectIdComparer());
        for (auto* object : objectsWithScheduledWeakUnref) {
            objectManager->WeakUnrefObject(object);
        }
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TEpochRefCounter::TEpochRefCounter(TObjectId id)
    : ShardIndex_(GetShardIndex<NDetail::EpochRefCounterShardCount>(id))
{ }

int TEpochRefCounter::GetValue() const
{
    NDetail::AssertPersistentStateRead();

    auto* shard = &NDetail::EpochRefCounterShards_[ShardIndex_];
    auto guard = Guard(shard->Lock);

    return Epoch_ == NDetail::EpochContext->CurrentEpoch
        ? Value_
        : 0;
}

int TEpochRefCounter::Increment(int delta)
{
    NDetail::AssertPersistentStateRead();

    auto* shard = &NDetail::EpochRefCounterShards_[ShardIndex_];
    auto guard = Guard(shard->Lock);

    YT_ASSERT(Value_ >= 0);

    auto currentEpoch = GetCurrentEpoch();
    YT_ASSERT(currentEpoch != TEpoch());

    if (currentEpoch != Epoch_) {
        Value_ = 0;
        Epoch_ = currentEpoch;
    }

    auto result = (Value_ += delta);
    YT_ASSERT(result >= 0);
    return result;
}

void TEpochRefCounter::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ShardIndex_);
    Persist(context, Value_);
    Persist(context, Epoch_);
}

////////////////////////////////////////////////////////////////////////////////

TCellTag TObject::GetNativeCellTag() const
{
    return CellTagFromId(Id_);
}

EObjectType TObject::GetType() const
{
    return TypeFromId(Id_);
}

bool TObject::IsBuiltin() const
{
    return IsWellKnownId(Id_);
}

bool TObject::IsSequoia() const
{
    return IsSequoiaId(Id_);
}

int TObject::GetLifeStageVoteCount() const
{
    return LifeStageVoteCount_;
}

void TObject::ResetLifeStageVoteCount()
{
    LifeStageVoteCount_ = 0;
}

int TObject::IncrementLifeStageVoteCount()
{
    return ++LifeStageVoteCount_;
}

int TObject::GetObjectEphemeralRefCounter() const
{
    return EphemeralRefCounter_.GetValue();
}

TString TObject::GetLowercaseObjectName() const
{
    return Format("object %v", Id_);
}

TString TObject::GetCapitalizedObjectName() const
{
    return Format("Object %v", Id_);
}

TString TObject::GetObjectPath() const
{
    return "";
}

const TAttributeSet* TObject::GetAttributes() const
{
    return Attributes_.get();
}

TAttributeSet* TObject::GetMutableAttributes()
{
    if (!Attributes_) {
        Attributes_ = std::make_unique<TAttributeSet>();
    }
    return Attributes_.get();
}

void TObject::ClearAttributes()
{
    Attributes_.reset();
}

const NYson::TYsonString* TObject::FindAttribute(const TString& key) const
{
    if (!Attributes_) {
        return nullptr;
    }

    const auto& attributeMap = Attributes_->Attributes();
    auto it = attributeMap.find(key);
    return it != attributeMap.end()
        ? &it->second
        : nullptr;
}

void TObject::RememberAevum()
{
    if (IsSequoia()) {
        SetAevum(GetCurrentAevum());
    }
}

int TObject::GetGCWeight() const
{
    return 10;
}

void TObject::CheckInvariants(TBootstrap* bootstrap) const
{
    YT_VERIFY(RefCounter_ >= 0);
    YT_VERIFY(WeakRefCounter_ >= 0);
    YT_VERIFY(ImportRefCounter_ >= 0);
    YT_VERIFY(LifeStageVoteCount_ >= 0);

    {
        const auto& multicellManager = bootstrap->GetMulticellManager();
        if (LifeStageVoteCount_ == multicellManager->GetCellCount()) {
            static const THashSet<EObjectLifeStage> allowedLifeStages = {
                EObjectLifeStage::CreationPreCommitted,
                EObjectLifeStage::CreationCommitted,
                EObjectLifeStage::RemovalAwaitingCellsSync,
                EObjectLifeStage::RemovalPreCommitted,
                EObjectLifeStage::RemovalCommitted,
            };
            YT_VERIFY(allowedLifeStages.contains(LifeStage_));
        } else {
            static const THashSet<EObjectLifeStage> allowedLifeStages = {
                EObjectLifeStage::CreationStarted,
                EObjectLifeStage::CreationPreCommitted,
                EObjectLifeStage::CreationCommitted,
                EObjectLifeStage::RemovalStarted,
                EObjectLifeStage::RemovalPreCommitted,
                EObjectLifeStage::RemovalAwaitingCellsSync,
                EObjectLifeStage::RemovalCommitted,
            };
            YT_VERIFY(allowedLifeStages.contains(LifeStage_));
        }

        const auto& garbageCollector = bootstrap->GetObjectManager()->GetGarbageCollector();
        auto* this_ = const_cast<TObject*>(this);
        YT_VERIFY(
            (LifeStage_ == EObjectLifeStage::RemovalAwaitingCellsSync) ==
            garbageCollector->GetRemovalAwaitingCellsSyncObjects().contains(this_));
    }

    YT_VERIFY(IsSequoia() == (Aevum_ != NSequoiaServer::EAevum::None));
}

void TObject::Save(NCellMaster::TSaveContext& context) const
{
    YT_VERIFY(!Flags_.Disposed);
    using NYT::Save;
    Save(context, RefCounter_);
    Save(context, WeakRefCounter_);
    Save(context, ImportRefCounter_);
    Save(context, LifeStageVoteCount_);
    Save(context, LifeStage_);
    if (Attributes_) {
        Save(context, true);
        Save(context, *Attributes_);
    } else {
        Save(context, false);
    }
    Save(context, IsForeign());
    Save(context, Aevum_);
}

void TObject::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter_);
    Load(context, WeakRefCounter_);
    Load(context, ImportRefCounter_);
    Load(context, LifeStageVoteCount_);
    Load(context, LifeStage_);
    if (Load<bool>(context)) {
        Attributes_ = std::make_unique<TAttributeSet>();
        Load(context, *Attributes_);
    }
    if (Load<bool>(context)) {
        SetForeign();
    }
    Load(context, Aevum_);
}

void TObject::SaveEctoplasm(TStreamSaveContext& context) const
{
    YT_VERIFY(RefCounter_ == 0);
    YT_VERIFY(!Flags_.Ghost);

    using NYT::Save;
    Save(context, Flags_.Foreign);
    Save(context, Flags_.Trunk);
    Save(context, WeakRefCounter_);
    Save(context, EphemeralRefCounter_);
}

void TObject::LoadEctoplasm(TStreamLoadContext& context)
{
    using NYT::Load;
    Flags_.Foreign = Load<bool>(context);
    Flags_.Trunk = Load<bool>(context);
    Load(context, WeakRefCounter_);
    Load(context, EphemeralRefCounter_);
}

////////////////////////////////////////////////////////////////////////////////

void TObjectIdFormatter::operator()(TStringBuilderBase* builder, const TObject* object) const
{
    FormatValue(builder, object->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

void TStrongObjectPtrContext::Ref(TObject* object)
{
    YT_VERIFY(IsInMutation());
    NDetail::Bootstrap->GetObjectManager()->RefObject(object);
}

void TStrongObjectPtrContext::Unref(TObject* object)
{
    YT_VERIFY(IsInMutation() || NDetail::IsInTeardown());
    if (IsInMutation()) {
        NDetail::ObjectsWithScheduledUnref.push_back(object);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWeakObjectPtrContext::Ref(TObject* object)
{
    YT_VERIFY(IsInMutation());
    NDetail::Bootstrap->GetObjectManager()->WeakRefObject(object);
}

void TWeakObjectPtrContext::Unref(TObject* object)
{
    YT_VERIFY(IsInMutation() || NDetail::IsInTeardown());
    if (IsInMutation()) {
        NDetail::ObjectsWithScheduledWeakUnref.push_back(object);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEphemeralObjectPtrContext TEphemeralObjectPtrContext::Capture()
{
    YT_VERIFY(NDetail::EpochContext->EphemeralPtrUnrefInvoker);
    return {
        NDetail::Bootstrap->GetObjectManager(),
        NDetail::EpochContext->CurrentEpoch,
        NDetail::EpochContext->EphemeralPtrUnrefInvoker
    };
};

bool TEphemeralObjectPtrContext::IsCurrent() const
{
    return
        NDetail::IsInAutomatonThread() &&
        ObjectManager == NDetail::Bootstrap->GetObjectManager() &&
        Epoch == NDetail::EpochContext->CurrentEpoch;
}

void TEphemeralObjectPtrContext::Ref(TObject* object)
{
    NDetail::AssertPersistentStateRead();
    ObjectManager->EphemeralRefObject(object);
}

void TEphemeralObjectPtrContext::Unref(TObject* object)
{
    if (auto epochContext = NDetail::EpochContext) {
        if (Epoch != epochContext->CurrentEpoch) {
            return;
        }
        ObjectManager->EphemeralUnrefObject(object);
    } else {
        ObjectManager->EphemeralUnrefObject(object, Epoch);
    }
}

////////////////////////////////////////////////////////////////////////////////

void BeginEpoch()
{
    NDetail::VerifyAutomatonThreadAffinity();

    YT_VERIFY(NDetail::EpochContext->CurrentEpoch == TEpoch());

    NDetail::EpochContext->EphemeralPtrUnrefInvoker = NDetail::Bootstrap
        ->GetHydraFacade()
        ->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::EphemeralPtrUnref);
    NDetail::EpochContext->CurrentEpoch = ++NDetail::EpochContext->CurrentEpochCounter;
}

void EndEpoch()
{
    NDetail::VerifyAutomatonThreadAffinity();

    NDetail::EpochContext->EphemeralPtrUnrefInvoker.Reset();
    NDetail::EpochContext->CurrentEpoch = TEpoch();
}

TEpoch GetCurrentEpoch()
{
    NDetail::AssertPersistentStateRead();

    return NDetail::EpochContext->CurrentEpoch;
}

void SetupMasterBootstrap(NCellMaster::TBootstrap* bootstrap)
{
    YT_VERIFY(!NDetail::Bootstrap);

    NDetail::Bootstrap = bootstrap;
}

void SetupAutomatonThread()
{
    YT_VERIFY(!NDetail::InAutomatonThread);

    NDetail::InAutomatonThread = true;
}

void SetupEpochContext(TEpochContextPtr epochContext)
{
    YT_VERIFY(!NDetail::EpochContext);

    NDetail::EpochContext = std::move(epochContext);
}

void ResetAll()
{
    NDetail::Bootstrap = nullptr;
    NDetail::InAutomatonThread = false;
    NDetail::EpochContext = nullptr;
}

void BeginMutation()
{
    NDetail::AssertAutomatonThreadAffinity();
    YT_ASSERT(!NDetail::IsInTeardown());

    if (++NDetail::InMutationCounter == 1) {
        YT_VERIFY(NDetail::ObjectsWithScheduledUnref.empty());
        YT_VERIFY(NDetail::ObjectsWithScheduledWeakUnref.empty());
    }
}

void EndMutation()
{
    NDetail::AssertAutomatonThreadAffinity();
    YT_VERIFY(IsInMutation());
    YT_VERIFY(!NDetail::IsInTeardown());

    NDetail::DoFlushObjectUnrefs();
    YT_VERIFY(--NDetail::InMutationCounter >= 0);
}

bool IsInMutation()
{
    return NDetail::InMutationCounter > 0;
}

void BeginTeardown()
{
    NDetail::VerifyAutomatonThreadAffinity();
    YT_VERIFY(!IsInMutation());
    YT_VERIFY(!NDetail::IsInTeardown());

    NDetail::InTeardownFlag = true;
}

void EndTeardown()
{
    NDetail::VerifyAutomatonThreadAffinity();
    YT_VERIFY(!IsInMutation());
    YT_VERIFY(NDetail::IsInTeardown());

    NDetail::InTeardownFlag = false;
}

void FlushObjectUnrefs()
{
    NDetail::AssertAutomatonThreadAffinity();
    YT_VERIFY(IsInMutation());
    YT_VERIFY(!NDetail::IsInTeardown());

    NDetail::DoFlushObjectUnrefs();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
