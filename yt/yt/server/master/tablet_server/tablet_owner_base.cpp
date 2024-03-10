#include "tablet_owner_base.h"

#include "tablet_cell_bundle.h"
#include "tablet_base.h"

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NYson;

using NHydra::GetCurrentMutationContext;

////////////////////////////////////////////////////////////////////////////////

void TTabletOwnerBase::TTabletOwnerAttributes::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Tablets);
    Save(context, TabletCountByState);
    Save(context, TabletCountByExpectedState);
    Save(context, TabletMasterMemoryUsage);
    Save(context, TabletErrorCount);
    Save(context, MountPath);
    Save(context, ExternalTabletResourceUsage);
    Save(context, ActualTabletState);
    Save(context, ExpectedTabletState);
    Save(context, LastMountTransactionId);
    Save(context, PrimaryLastMountTransactionId);
    Save(context, CurrentMountTransactionId);
    Save(context, TabletStatistics);
    Save(context, InMemoryMode);
    Save(context, SettingsUpdateRevision);
    Save(context, RemountNeededTabletCount);
}

void TTabletOwnerBase::TTabletOwnerAttributes::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Tablets);
    Load(context, TabletCountByState);
    Load(context, TabletCountByExpectedState);
    Load(context, TabletMasterMemoryUsage);
    Load(context, TabletErrorCount);
    Load(context, MountPath);
    Load(context, ExternalTabletResourceUsage);
    Load(context, ActualTabletState);
    Load(context, ExpectedTabletState);
    Load(context, LastMountTransactionId);
    Load(context, PrimaryLastMountTransactionId);
    Load(context, CurrentMountTransactionId);
    Load(context, TabletStatistics);
    Load(context, InMemoryMode);
    Load(context, SettingsUpdateRevision);
    Load(context, RemountNeededTabletCount);
}

void TTabletOwnerBase::TTabletOwnerAttributes::CopyFrom(const TTabletOwnerAttributes* other)
{
    InMemoryMode = other->InMemoryMode;
}

void TTabletOwnerBase::TTabletOwnerAttributes::BeginCopy(NCypressServer::TBeginCopyContext* context) const
{
    using NYT::Save;

    Save(*context, InMemoryMode);
}

void TTabletOwnerBase::TTabletOwnerAttributes::EndCopy(NCypressServer::TEndCopyContext* context)
{
    using NYT::Load;

    Load(*context, InMemoryMode);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletOwnerBase::LoadTabletStatisticsCompat(TLoadContext& context)
{
    using NYT::Load;

    if (!HasCustomTabletOwnerAttributes()) {
        InitializeCustomTabletOwnerAttributes();
    }
    Load(context, TabletOwnerAttributes_->TabletStatistics);
}

void TTabletOwnerBase::Save(TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;

    Save(context, TabletCellBundle_);
    TUniquePtrSerializer<>::Save(context, TabletOwnerAttributes_);
}

void TTabletOwnerBase::Load(TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;

    Load(context, TabletCellBundle_);
    TUniquePtrSerializer<>::Load(context, TabletOwnerAttributes_);
}

TTabletOwnerBase* TTabletOwnerBase::GetTrunkNode()
{
    return TChunkOwnerBase::GetTrunkNode()->As<TTabletOwnerBase>();
}

const TTabletOwnerBase* TTabletOwnerBase::GetTrunkNode() const
{
    return TChunkOwnerBase::GetTrunkNode()->As<TTabletOwnerBase>();
}

TDetailedMasterMemory TTabletOwnerBase::GetDetailedMasterMemoryUsage() const
{
    auto result = TChunkOwnerBase::GetDetailedMasterMemoryUsage();
    result[EMasterMemoryType::Tablets] += GetTabletMasterMemoryUsage();
    return result;
}

void TTabletOwnerBase::RecomputeTabletMasterMemoryUsage()
{
    i64 masterMemoryUsage = 0;
    for (const auto* tablet : Tablets()) {
        masterMemoryUsage += tablet->GetTabletMasterMemoryUsage();
    }
    SetTabletMasterMemoryUsage(masterMemoryUsage);
}

void TTabletOwnerBase::RecomputeTabletErrorCount()
{
    if (!HasCustomTabletOwnerAttributes()) {
        return;
    }

    int tabletErrorCount = 0;
    for (auto* tablet : Tablets()) {
        tabletErrorCount += tablet->GetTabletErrorCount();
    }
    SetTabletErrorCount(tabletErrorCount);
}

TTabletResources TTabletOwnerBase::GetTabletResourceUsage() const
{
    int tabletCount = 0;
    i64 tabletStaticMemory = 0;

    if (IsTrunk()) {
        tabletCount = Tablets().size();
        for (const auto* tablet : Tablets()) {
            if (tablet->GetState() != ETabletState::Unmounted) {
                tabletStaticMemory += tablet->GetTabletStaticMemorySize();
            }
        }
    }

    auto resourceUsage = TTabletResources()
        .SetTabletCount(tabletCount)
        .SetTabletStaticMemory(tabletStaticMemory);

    return resourceUsage + GetExternalTabletResourceUsage();
}

ETabletState TTabletOwnerBase::GetTabletState() const
{
    if (GetLastMountTransactionId()) {
        return ETabletState::Transient;
    }

    return GetActualTabletState();
}

ETabletState TTabletOwnerBase::ComputeActualTabletState() const
{
    auto* trunkNode = GetTrunkNode();
    if (trunkNode->Tablets().empty()) {
        YT_ABORT();
        return ETabletState::None;
    }
    for (auto state : TEnumTraits<ETabletState>::GetDomainValues()) {
        if (trunkNode->TabletCountByState().IsValidIndex(state)) {
            if (std::ssize(trunkNode->Tablets()) == trunkNode->TabletCountByState()[state]) {
                return state;
            }
        }
    }
    return ETabletState::Mixed;
}

void TTabletOwnerBase::UpdateExpectedTabletState(ETabletState state)
{
    auto current = GetExpectedTabletState();

    YT_ASSERT(current == ETabletState::Frozen ||
        current == ETabletState::Mounted ||
        current == ETabletState::Unmounted);
    YT_ASSERT(state == ETabletState::Frozen ||
        state == ETabletState::Mounted);

    if (state == ETabletState::Mounted ||
        (state == ETabletState::Frozen && current != ETabletState::Mounted))
    {
        SetExpectedTabletState(state);
    }
}

void TTabletOwnerBase::ValidateNoCurrentMountTransaction(TStringBuf message) const
{
    const auto* trunkNode = GetTrunkNode();
    if (auto transactionId = trunkNode->GetCurrentMountTransactionId()) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::InvalidTabletState, "%v since node is locked by mount-unmount operation", message)
            << TErrorAttribute("current_mount_transaction_id", transactionId);
    }
}

void TTabletOwnerBase::ValidateTabletStateFixed(TStringBuf message) const
{
    ValidateNoCurrentMountTransaction(message);

    const auto* trunkNode = GetTrunkNode();
    if (auto transactionId = trunkNode->GetLastMountTransactionId()) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::InvalidTabletState, "%v since some tablets are in transient state", message)
            << TErrorAttribute("last_mount_transaction_id", transactionId)
            << TErrorAttribute("expected_tablet_state", trunkNode->GetExpectedTabletState());
    }
}

void TTabletOwnerBase::ValidateExpectedTabletState(TStringBuf message, bool allowFrozen) const
{
    ValidateTabletStateFixed(message);

    const auto* trunkNode = GetTrunkNode();
    auto state = trunkNode->GetExpectedTabletState();
    if (!(state == ETabletState::Unmounted || (allowFrozen && state == ETabletState::Frozen))) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::InvalidTabletState, "%v since not all tablets are %v",
            message,
            allowFrozen ? "frozen or unmounted" : "unmounted")
            << TErrorAttribute("actual_tablet_state", trunkNode->GetActualTabletState())
            << TErrorAttribute("expected_tablet_state", trunkNode->GetExpectedTabletState());
    }
}

void TTabletOwnerBase::ValidateAllTabletsFrozenOrUnmounted(TStringBuf message) const
{
    ValidateExpectedTabletState(message, true);
}

void TTabletOwnerBase::ValidateAllTabletsUnmounted(TStringBuf message) const
{
    ValidateExpectedTabletState(message, false);
}

void TTabletOwnerBase::ValidateMount() const
{ }

void TTabletOwnerBase::ValidateUnmount() const
{ }

void TTabletOwnerBase::ValidateRemount() const
{ }

void TTabletOwnerBase::ValidateFreeze() const
{ }

void TTabletOwnerBase::ValidateUnfreeze() const
{ }

void TTabletOwnerBase::ValidateReshard(
    const TBootstrap* /*bootstrap*/,
    int /*firstTabletIndex*/,
    int /*lastTabletIndex*/,
    int /*newTabletCount*/,
    const std::vector<NTableClient::TLegacyOwningKey>& /*pivotKeys*/,
    const std::vector<i64>& /*trimmedRowCounts*/) const
{ }

void TTabletOwnerBase::LockCurrentMountTransaction(TTransactionId transactionId)
{
    YT_ASSERT(!static_cast<bool>(GetCurrentMountTransactionId()));
    SetCurrentMountTransactionId(transactionId);
}

void TTabletOwnerBase::UnlockCurrentMountTransaction(TTransactionId transactionId)
{
    if (GetCurrentMountTransactionId() == transactionId) {
        SetCurrentMountTransactionId(TTransactionId());
    }
}

void TTabletOwnerBase::OnRemountNeeded()
{
    if (IsExternal()) {
        return;
    }

    auto revision = GetCurrentMutationContext()->GetVersion().ToRevision();
    SetSettingsUpdateRevision(revision);

    SetRemountNeededTabletCount(ssize(Tablets()) - TabletCountByState()[ETabletState::Unmounted]);
}

DEFINE_EXTRA_PROPERTY_HOLDER(TTabletOwnerBase, TTabletOwnerBase::TTabletOwnerAttributes, TabletOwnerAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
