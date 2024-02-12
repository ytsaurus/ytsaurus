#include "tablet_base.h"

#include "tablet_action.h"
#include "tablet_cell.h"
#include "tablet_owner_base.h"

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressClient;
using namespace NHiveServer;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NTransactionClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTabletServant::TTabletServant(bool isAuxiliary)
    : IsAuxiliary_(isAuxiliary)
{ }

bool TTabletServant::IsAuxiliary() const
{
    return IsAuxiliary_;
}

TTabletServant::operator bool() const
{
    return static_cast<bool>(Cell_);
}

void TTabletServant::Clear()
{
    Cell_ = nullptr;
    State_ = ETabletState::Unmounted;
    MountRevision_ = {};
    MountTime_ = {};
    MovementRole_ = ESmoothMovementRole::None;
    MovementStage_ = ESmoothMovementStage::None;
}

void TTabletServant::Swap(TTabletServant* other)
{
    std::swap(Cell_, other->Cell_);
    std::swap(State_, other->State_);
    std::swap(MountRevision_, other->MountRevision_);
    std::swap(MountTime_, other->MountTime_);
    std::swap(MovementRole_, other->MovementRole_);
    std::swap(MovementStage_, other->MovementStage_);
}

void TTabletServant::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Cell_);
    Persist(context, State_);
    Persist(context, MountRevision_);
    Persist(context, MountTime_);

    // COMPAT(ifsmirnov)
    if (context.IsSave() || context.GetVersion() >= EMasterReign::SmoothTabletMovement) {
        Persist(context, MovementRole_);
        Persist(context, MovementStage_);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBase::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Index_);
    Save(context, InMemoryMode_);
    Save(context, Servant_);
    Save(context, AuxiliaryServant_);
    Save(context, SettingsRevision_);
    Save(context, WasForcefullyUnmounted_);
    Save(context, Action_);
    Save(context, StoresUpdatePreparedTransaction_);
    Save(context, TabletwiseAvenueEndpointId_);
    Save(context, Owner_);
    Save(context, State_);
    Save(context, ExpectedState_);
    Save(context, TabletErrorCount_);
    Save(context, NodeAvenueEndpointId_);
}

void TTabletBase::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Index_);
    Load(context, InMemoryMode_);
    // COMPAT(ifsmirnov)
    if (context.GetVersion() < EMasterReign::TabletServants) {
        auto* cell = Load<TTabletCell*>(context);
        Servant_.SetCell(cell);

        Servant_.SetMountRevision(Load<NHydra::TRevision>(context));

        Load(context, SettingsRevision_);

        // COMPAT(alexelexa)
        if (context.GetVersion() >= EMasterReign::AddTabletMountTime) {
            Servant_.SetMountTime(Load<TInstant>(context));
        }
    } else {
        Load(context, Servant_);

        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= EMasterReign::SmoothTabletMovement) {
            Load(context, AuxiliaryServant_);
        }

        Load(context, SettingsRevision_);
    }

    Load(context, WasForcefullyUnmounted_);
    Load(context, Action_);
    Load(context, StoresUpdatePreparedTransaction_);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= EMasterReign::SmoothTabletMovement) {
        Load(context, TabletwiseAvenueEndpointId_);
    }

    Load(context, Owner_);
    Load(context, State_);
    Load(context, ExpectedState_);
    Load(context, TabletErrorCount_);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() < EMasterReign::TabletServants) {
        Servant_.SetState(State_);
    }

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= EMasterReign::AvenuesInTabletManager) {
        Load(context, NodeAvenueEndpointId_);
    }
}

ETabletState TTabletBase::GetState() const
{
    return State_;
}

void TTabletBase::SetState(ETabletState state)
{
    if (Owner_) {
        auto* owner = Owner_->GetTrunkNode();
        YT_VERIFY(owner->TabletCountByState()[State_] > 0);
        --owner->MutableTabletCountByState()[State_];
        ++owner->MutableTabletCountByState()[state];
    }

    if (!Action_) {
        SetExpectedState(state);
    }

    State_ = state;
}

ETabletState TTabletBase::GetExpectedState() const
{
    return ExpectedState_;
}

void TTabletBase::SetExpectedState(ETabletState state)
{
    if (Owner_) {
        auto* owner = Owner_->GetTrunkNode();
        YT_VERIFY(owner->TabletCountByExpectedState()[ExpectedState_] > 0);
        --owner->MutableTabletCountByExpectedState()[ExpectedState_];
        ++owner->MutableTabletCountByExpectedState()[state];
    }
    ExpectedState_ = state;
}

TTabletOwnerBase* TTabletBase::GetOwner() const
{
    return Owner_;
}

void TTabletBase::SetOwner(TTabletOwnerBase* owner)
{
    if (Owner_) {
        YT_VERIFY(Owner_->GetTrunkNode()->TabletCountByState()[State_] > 0);
        YT_VERIFY(Owner_->GetTrunkNode()->TabletCountByExpectedState()[ExpectedState_] > 0);
        --Owner_->GetTrunkNode()->MutableTabletCountByState()[State_];
        --Owner_->GetTrunkNode()->MutableTabletCountByExpectedState()[ExpectedState_];

        int restTabletErrorCount = Owner_->GetTabletErrorCount() - GetTabletErrorCount();
        YT_ASSERT(restTabletErrorCount >= 0);
        Owner_->SetTabletErrorCount(restTabletErrorCount);
    }
    if (owner) {
        YT_VERIFY(owner->IsTrunk());
        ++owner->MutableTabletCountByState()[State_];
        ++owner->MutableTabletCountByExpectedState()[ExpectedState_];

        owner->SetTabletErrorCount(owner->GetTabletErrorCount() + GetTabletErrorCount());
    }
    Owner_ = owner;
}

TCompactVector<TTabletCell*, 2> TTabletBase::GetCells() const
{
    TCompactVector<TTabletCell*, 2> result;

    if (auto* cell = Servant_.GetCell()) {
        result.push_back(cell);
    }

    if (auto* cell = AuxiliaryServant_.GetCell()) {
        result.push_back(cell);
    }

    return result;
}

void TTabletBase::CopyFrom(const TTabletBase& other)
{
    YT_VERIFY(State_ == ETabletState::Unmounted);
    YT_VERIFY(!Servant_.GetCell());

    Index_ = other.Index_;
    InMemoryMode_ = other.InMemoryMode_;
}

void TTabletBase::ValidateMountRevision(NHydra::TRevision mountRevision)
{
    if (Servant_.GetMountRevision() != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            Id_,
            Servant_.GetMountRevision(),
            mountRevision);
    }
}

bool TTabletBase::IsActive() const
{
    return
        State_ == ETabletState::Mounting ||
        State_ == ETabletState::FrozenMounting ||
        State_ == ETabletState::Mounted ||
        State_ == ETabletState::Freezing ||
        State_ == ETabletState::Frozen ||
        State_ == ETabletState::Unfreezing;
}

NChunkServer::TChunkList* TTabletBase::GetChunkList()
{
    return GetChunkList(EChunkListContentType::Main);
}

const NChunkServer::TChunkList* TTabletBase::GetChunkList() const
{
    return GetChunkList(EChunkListContentType::Main);
}

NChunkServer::TChunkList* TTabletBase::GetHunkChunkList()
{
    return GetChunkList(EChunkListContentType::Hunk);
}

const NChunkServer::TChunkList* TTabletBase::GetHunkChunkList() const
{
    return GetChunkList(EChunkListContentType::Hunk);
}

TChunkList* TTabletBase::GetChunkList(EChunkListContentType type)
{
    if (auto* rootChunkList = Owner_->GetTrunkNode()->GetChunkList(type)) {
        return rootChunkList->Children()[Index_]->AsChunkList();
    } else {
        return nullptr;
    }
}

const TChunkList* TTabletBase::GetChunkList(EChunkListContentType type) const
{
    return const_cast<TTabletBase*>(this)->GetChunkList(type);
}


TTabletServant* TTabletBase::FindServant(TTabletCellId cellId)
{
    if (GetObjectId(Servant_.GetCell()) == cellId) {
        return &Servant_;
    } else if (GetObjectId(AuxiliaryServant_.GetCell()) == cellId) {
        return &AuxiliaryServant_;
    } else {
        return nullptr;
    }
}

const TTabletServant* TTabletBase::FindServant(TTabletCellId cellId) const
{
    return const_cast<TTabletBase*>(this)->FindServant(cellId);
}

TTabletServant* TTabletBase::FindServant(NHydra::TRevision mountRevision)
{
    if (Servant_.GetMountRevision() == mountRevision) {
        return &Servant_;
    } else if (AuxiliaryServant_.GetMountRevision() == mountRevision) {
        return &AuxiliaryServant_;
    } else {
        return nullptr;
    }
}

const TTabletServant* TTabletBase::FindServant(NHydra::TRevision mountRevision) const
{
    return const_cast<TTabletBase*>(this)->FindServant(mountRevision);
}

TTabletCell* TTabletBase::GetCell() const
{
    return Servant_.GetCell();
}

i64 TTabletBase::GetTabletStaticMemorySize(EInMemoryMode mode) const
{
    // TODO(savrus) consider lookup hash table.

    const auto& statistics = GetChunkList()->Statistics();
    switch (mode) {
        case EInMemoryMode::Compressed:
            return statistics.CompressedDataSize;
        case EInMemoryMode::Uncompressed:
            return statistics.UncompressedDataSize;
        case EInMemoryMode::None:
            return 0;
        default:
            YT_ABORT();
    }
}

i64 TTabletBase::GetTabletStaticMemorySize() const
{
    return GetTabletStaticMemorySize(GetInMemoryMode());
}

i64 TTabletBase::GetTabletMasterMemoryUsage() const
{
    return sizeof(TTabletBase);
}

void TTabletBase::ValidateNotSmoothlyMoved(TStringBuf message) const
{
    if (const auto* action = GetAction()) {
        if (action->GetKind() == ETabletActionKind::SmoothMove) {
            THROW_ERROR_EXCEPTION("%v since tablet %v is being moved to other cell",
                message,
                GetId());
        }
    }

    YT_VERIFY(!AuxiliaryServant());
}

void TTabletBase::ValidateMount(bool freeze)
{
    if (State_ != ETabletState::Unmounted && (freeze
        ? State_ != ETabletState::Frozen &&
            State_ != ETabletState::Freezing &&
            State_ != ETabletState::FrozenMounting
        : State_ != ETabletState::Mounted &&
            State_ != ETabletState::Mounting &&
            State_ != ETabletState::Unfreezing))
    {
        THROW_ERROR_EXCEPTION("Cannot mount tablet %v in %Qlv state",
            Id_,
            State_);
    }

    std::vector<TChunkTree*> stores;
    for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
        if (auto* chunkList = GetChunkList(contentType)) {
            EnumerateStoresInChunkTree(chunkList, &stores);
        }
    }

    THashSet<TObjectId> storeSet;
    storeSet.reserve(stores.size());
    for (auto* store : stores) {
        if (!storeSet.insert(store->GetId()).second) {
            THROW_ERROR_EXCEPTION("Cannot mount %v: tablet %v contains duplicate store %v of type %Qlv",
                GetOwner()->GetType(),
                Id_,
                store->GetId(),
                store->GetType());
        }
    }
}

void TTabletBase::ValidateUnmount()
{
    if (State_ != ETabletState::Mounted &&
        State_ != ETabletState::Frozen &&
        State_ != ETabletState::Unmounted &&
        State_ != ETabletState::Unmounting)
    {
        THROW_ERROR_EXCEPTION("Cannot unmount tablet %v in %Qlv state",
            Id_,
            State_);
    }
}

void TTabletBase::ValidateFreeze() const
{
    if (State_ != ETabletState::Mounted &&
        State_ != ETabletState::FrozenMounting &&
        State_ != ETabletState::Freezing &&
        State_ != ETabletState::Frozen)
    {
        THROW_ERROR_EXCEPTION("Cannot freeze tablet %v in %Qlv state",
            Id_,
            State_);
    }
}

void TTabletBase::ValidateUnfreeze() const
{
    if (State_ != ETabletState::Mounted &&
        State_ != ETabletState::Frozen &&
        State_ != ETabletState::Unfreezing)
    {
        THROW_ERROR_EXCEPTION("Cannot unfreeze tablet %v in %Qlv state",
            Id_,
            State_);
    }
}

void TTabletBase::ValidateReshard() const
{
    if (State_ != ETabletState::Unmounted) {
        THROW_ERROR_EXCEPTION("Cannot reshard table since tablet %v is not unmounted",
            Id_);
    }
}

void TTabletBase::ValidateReshardRemove() const
{ }

void TTabletBase::ValidateRemount() const
{
    ValidateNotSmoothlyMoved("Cannot remount tablet");
}

int TTabletBase::GetTabletErrorCount() const
{
    return TabletErrorCount_;
}

void TTabletBase::SetTabletErrorCount(int tabletErrorCount)
{
    if (Owner_) {
        int restTabletErrorCount = Owner_->GetTabletErrorCount() - GetTabletErrorCount();
        YT_ASSERT(restTabletErrorCount >= 0);
        Owner_->SetTabletErrorCount(restTabletErrorCount + tabletErrorCount);
    }

    TabletErrorCount_ = tabletErrorCount;
}

void TTabletBase::SetNodeAvenueEndpointId(NHiveServer::TAvenueEndpointId endpointId)
{
    YT_VERIFY(NHydra::HasHydraContext());

    NodeAvenueEndpointId_ = endpointId;
}

TEndpointId TTabletBase::GetNodeEndpointId() const
{
    YT_VERIFY(State_ != ETabletState::Unmounted);

    return NodeAvenueEndpointId_
        ? NodeAvenueEndpointId_
        : Servant_.GetCell()->GetId();
}

bool TTabletBase::IsMountedWithAvenue() const
{
    return static_cast<bool>(NodeAvenueEndpointId_);
}

void TTabletBase::CheckInvariants(NCellMaster::TBootstrap* bootstrap) const
{
    TObject::CheckInvariants(bootstrap);

    YT_VERIFY(GetState() == Servant().GetState());

    if (GetState() == ETabletState::Unmounted) {
        YT_VERIFY(Servant().GetState() == ETabletState::Unmounted);
        YT_VERIFY(AuxiliaryServant().GetState() == ETabletState::Unmounted);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
