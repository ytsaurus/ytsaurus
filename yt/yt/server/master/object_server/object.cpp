#include "object.h"

#include "garbage_collector.h"
#include "object.h"
#include "object_manager.h"

#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/client/object_client/helpers.h>

#include <util/generic/algorithm.h>

#include <library/cpp/yt/misc/tls.h>

#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
#include <yt/yt/core/misc/random.h>
#endif

namespace NYT::NObjectServer {

using namespace NObjectClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NHydra;
using namespace NSequoiaServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TThreadState
{
    NCellMaster::TBootstrap* Bootstrap;
    TEpochContextPtr EpochContext;
    bool IsAutomatonThread;

    bool InTeardownFlag = false;
    int InMutationCounter = 0;

    std::vector<TObject*> ObjectsWithScheduledUnref;
    std::vector<TObject*> ObjectsWithScheduledWeakUnref;

    bool IsInMutation() const
    {
        return InMutationCounter > 0;
    }

    bool IsInTeardown() const
    {
        return InTeardownFlag;
    }

    void FlushObjectUnrefs()
    {
        YT_VERIFY(IsInMutation());
        YT_VERIFY(!IsInTeardown());

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
};

YT_DEFINE_THREAD_LOCAL(std::optional<TThreadState>, ThreadState);

TThreadState* TryGetThreadState()
{
    auto& threadState = ThreadState();
    return threadState ? &*threadState : nullptr;
}

TThreadState* GetThreadState()
{
    auto& threadState = ThreadState();
    YT_VERIFY(threadState);
    return &*threadState;
}

////////////////////////////////////////////////////////////////////////////////

struct TEpochRefCounterShard
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
};

constexpr int EpochRefCounterShardCount = 256;
static_assert(IsPowerOf2(EpochRefCounterShardCount), "EpochRefCounterShardCount must be a power of 2");
std::array<TEpochRefCounterShard, EpochRefCounterShardCount> EpochRefCounterShards;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TEpochRefCounter::TEpochRefCounter(TObjectId id)
    : ShardIndex_(GetShardIndex<NDetail::EpochRefCounterShardCount>(id))
{ }

int TEpochRefCounter::GetValue() const
{
    AssertPersistentStateRead();

    auto* shard = &NDetail::EpochRefCounterShards[ShardIndex_];
    auto guard = Guard(shard->Lock);
    auto* threadState = NDetail::GetThreadState();
    return Epoch_ == threadState->EpochContext->CurrentEpoch
        ? Value_
        : 0;
}

int TEpochRefCounter::Increment(int delta)
{
    AssertPersistentStateRead();

    auto* shard = &NDetail::EpochRefCounterShards[ShardIndex_];
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

#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING

YT_PREVENT_TLS_CACHING TRopSanTag TObject::GenerateRopSanTag()
{
    thread_local TRandomGenerator generator(::time(nullptr));
    return generator.Generate<TRopSanTag>();
}

#endif

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

std::string TObject::GetLowercaseObjectName() const
{
    return Format("object %v", Id_);
}

std::string TObject::GetCapitalizedObjectName() const
{
    return Format("Object %v", Id_);
}

TYPath TObject::GetObjectPath() const
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

const NYson::TYsonString* TObject::FindAttribute(const std::string& key) const
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

TRevision TObject::GetRevision() const
{
    return Max(AttributeRevision_, ContentRevision_);
}

void TObject::SetModified(EModificationType modificationType)
{
    auto* hydraContext = GetCurrentHydraContext();
    YT_VERIFY(hydraContext);
    auto currentRevision = hydraContext->GetVersion().ToRevision();

    switch (modificationType) {
        case EModificationType::Attributes: {
            AttributeRevision_ = currentRevision;
            break;
        }
        case EModificationType::Content: {
            ContentRevision_ = currentRevision;
            break;
        }
        default:
            YT_ABORT();
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
    Save(context, AttributeRevision_);
    Save(context, ContentRevision_);
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
    if (context.GetVersion() < EMasterReign::RipAevum) {
        Load<int>(context);
    }
    Load(context, AttributeRevision_);
    Load(context, ContentRevision_);
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
#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
    Save(context, RopSanTag_);
#endif
}

void TObject::LoadEctoplasm(TStreamLoadContext& context)
{
    using NYT::Load;
    Flags_.Foreign = Load<bool>(context);
    Flags_.Trunk = Load<bool>(context);
    Load(context, WeakRefCounter_);
    Load(context, EphemeralRefCounter_);
#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
    Load(context, RopSanTag_);
#endif
}

////////////////////////////////////////////////////////////////////////////////

void TObjectIdFormatter::operator()(TStringBuilderBase* builder, const TObject* object) const
{
    FormatValue(builder, object->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

void TStrongObjectPtrContext::Ref(TObject* object)
{
    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(threadState->IsInMutation());
    threadState->Bootstrap->GetObjectManager()->RefObject(object);
}

void TStrongObjectPtrContext::Unref(TObject* object)
{
    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(threadState->IsInMutation() || threadState->IsInTeardown());
    if (threadState->IsInMutation()) {
        threadState->ObjectsWithScheduledUnref.push_back(object);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWeakObjectPtrContext::Ref(TObject* object)
{
    const auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(threadState->IsInMutation());
    threadState->Bootstrap->GetObjectManager()->WeakRefObject(object);
}

void TWeakObjectPtrContext::Unref(TObject* object)
{
    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(threadState->IsInMutation() || threadState->IsInTeardown());
    if (threadState->IsInMutation()) {
        threadState->ObjectsWithScheduledWeakUnref.push_back(object);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEphemeralObjectPtrContext TEphemeralObjectPtrContext::Capture()
{
    const auto* threadState = NDetail::GetThreadState();
    const auto& epochContext = threadState->EpochContext;
    YT_VERIFY(epochContext->EphemeralPtrUnrefInvoker);
    return {
        .ObjectManager = threadState->Bootstrap->GetObjectManager(),
        .Epoch = epochContext->CurrentEpoch,
        .EphemeralPtrUnrefInvoker = epochContext->EphemeralPtrUnrefInvoker,
    };
}

bool TEphemeralObjectPtrContext::IsCurrent() const
{
    // For tests only.
    const auto* threadState = NDetail::TryGetThreadState();
    return
        threadState &&
        threadState->IsAutomatonThread &&
        ObjectManager == threadState->Bootstrap->GetObjectManager() &&
        Epoch == threadState->EpochContext->CurrentEpoch;
}

void TEphemeralObjectPtrContext::Ref(TObject* object)
{
    AssertPersistentStateRead();
    ObjectManager->EphemeralRefObject(object);
}

void TEphemeralObjectPtrContext::Unref(TObject* object)
{
    if (const auto* threadState = NDetail::TryGetThreadState()) {
        if (Epoch != threadState->EpochContext->CurrentEpoch) {
            return;
        }
        ObjectManager->EphemeralUnrefObject(object);
    } else {
        ObjectManager->EphemeralUnrefObject(object, Epoch);
    }
}

////////////////////////////////////////////////////////////////////////////////

void InitializeMasterStateThread(
    NCellMaster::TBootstrap* bootstrap,
    TEpochContextPtr epochContext,
    bool isAutomatonThread)
{
    YT_VERIFY(bootstrap);
    YT_VERIFY(epochContext);

    auto& threadState = NDetail::ThreadState();
    YT_VERIFY(!threadState);
    threadState = {
        .Bootstrap = bootstrap,
        .EpochContext = std::move(epochContext),
        .IsAutomatonThread = isAutomatonThread,
    };
}

void FinalizeMasterStateThread()
{
    auto& threadState = NDetail::ThreadState();
    YT_VERIFY(threadState);
    threadState.reset();
}

void BeginEpoch()
{
    VerifyAutomatonThreadAffinity();


    const auto* threadState = NDetail::GetThreadState();
    const auto& epochContext = threadState->EpochContext;

    YT_VERIFY(epochContext->CurrentEpoch == TEpoch());

    epochContext->EphemeralPtrUnrefInvoker = threadState->Bootstrap
        ->GetHydraFacade()
        ->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::EphemeralPtrUnref);
    epochContext->CurrentEpoch = ++epochContext->CurrentEpochCounter;
}

void EndEpoch()
{
    VerifyAutomatonThreadAffinity();

    const auto* threadState = NDetail::GetThreadState();
    const auto& epochContext = threadState->EpochContext;

    epochContext->EphemeralPtrUnrefInvoker.Reset();
    epochContext->CurrentEpoch = TEpoch();
}

TEpoch GetCurrentEpoch()
{
    AssertPersistentStateRead();

    const auto* threadState = NDetail::GetThreadState();
    return threadState->EpochContext->CurrentEpoch;
}

void BeginMutation()
{
    AssertAutomatonThreadAffinity();

    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(!threadState->IsInTeardown());

    if (++threadState->InMutationCounter == 1) {
        YT_VERIFY(threadState->ObjectsWithScheduledUnref.empty());
        YT_VERIFY(threadState->ObjectsWithScheduledWeakUnref.empty());
    }
}

void EndMutation()
{
    AssertAutomatonThreadAffinity();

    auto* threadState = NDetail::GetThreadState();
    threadState->FlushObjectUnrefs();
    YT_VERIFY(--threadState->InMutationCounter >= 0);
}

bool IsInMutation()
{
    const auto* threadState = NDetail::TryGetThreadState();
    return threadState && threadState->IsInMutation() > 0;
}

void BeginTeardown()
{
    VerifyAutomatonThreadAffinity();

    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(!threadState->IsInMutation());
    YT_VERIFY(!threadState->IsInTeardown());
    threadState->InTeardownFlag = true;
}

void EndTeardown()
{
    VerifyAutomatonThreadAffinity();

    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(!threadState->IsInMutation());
    YT_VERIFY(threadState->IsInTeardown());
    threadState->InTeardownFlag = false;
}

void FlushObjectUnrefs()
{
    AssertAutomatonThreadAffinity();

    auto* threadState = NDetail::GetThreadState();
    threadState->FlushObjectUnrefs();
}

void VerifyAutomatonThreadAffinity()
{
    auto* threadState = NDetail::GetThreadState();
    YT_VERIFY(threadState->IsAutomatonThread);
}

void VerifyPersistentStateRead()
{
    auto* threadState = NDetail::GetThreadState();
    // NB: IsInAutomatonThread is fork-aware while VerifyPersistentStateRead is not.
    if (!threadState->IsAutomatonThread) {
        threadState->Bootstrap->VerifyPersistentStateRead();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
