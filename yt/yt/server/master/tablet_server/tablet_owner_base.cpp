#include "tablet_owner_base.h"

#include "tablet_cell_bundle.h"
#include "tablet_base.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NYson;

using NHydra::GetCurrentMutationContext;

////////////////////////////////////////////////////////////////////////////////

void TTabletOwnerBase::TTabletOwnerAttributes::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Tablets);
    PHOENIX_REGISTER_FIELD(2, TabletCountByState);
    PHOENIX_REGISTER_FIELD(3, TabletCountByExpectedState);
    PHOENIX_REGISTER_FIELD(4, TabletMasterMemoryUsage);
    PHOENIX_REGISTER_FIELD(5, TabletErrorCount);
    PHOENIX_REGISTER_FIELD(6, MountPath);
    PHOENIX_REGISTER_FIELD(7, ExternalTabletResourceUsage);
    PHOENIX_REGISTER_FIELD(8, ActualTabletState);
    PHOENIX_REGISTER_FIELD(9, ExpectedTabletState);
    PHOENIX_REGISTER_FIELD(10, LastMountTransactionId);
    PHOENIX_REGISTER_FIELD(11, PrimaryLastMountTransactionId);
    PHOENIX_REGISTER_FIELD(12, CurrentMountTransactionId);
    PHOENIX_REGISTER_FIELD(13, TabletStatistics);
    PHOENIX_REGISTER_FIELD(14, InMemoryMode);
    PHOENIX_REGISTER_FIELD(15, SettingsUpdateRevision);
    PHOENIX_REGISTER_FIELD(16, RemountNeededTabletCount);
}

PHOENIX_DEFINE_TYPE(TTabletOwnerBase::TTabletOwnerAttributes);

void TTabletOwnerBase::TTabletOwnerAttributes::CopyFrom(const TTabletOwnerAttributes* other)
{
    InMemoryMode = other->InMemoryMode;
}

void TTabletOwnerBase::TTabletOwnerAttributes::SerializeNode(NCypressServer::TSerializeNodeContext* context) const
{
    using NYT::Save;

    Save(*context, InMemoryMode);
}

void TTabletOwnerBase::TTabletOwnerAttributes::MaterializeNode(NCypressServer::TMaterializeNodeContext* context)
{
    using NYT::Load;

    Load(*context, InMemoryMode);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletOwnerBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkOwnerBase>();

    PHOENIX_REGISTER_FIELD(1, TabletCellBundle_);
    PHOENIX_REGISTER_FIELD(2, TabletOwnerAttributes_,
        .template Serializer<TUniquePtrSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TTabletOwnerBase);

TTabletOwnerBase* TTabletOwnerBase::GetTrunkNode()
{
    return TChunkOwnerBase::GetTrunkNode()->As<TTabletOwnerBase>();
}

const TTabletOwnerBase* TTabletOwnerBase::GetTrunkNode() const
{
    return TChunkOwnerBase::GetTrunkNode()->As<TTabletOwnerBase>();
}

TTabletCellBundleRawPtr TTabletOwnerBase::GetTabletCellBundle() const
{
    return GetTrunkNode()->TabletCellBundle_.Get();
}

void TTabletOwnerBase::SetTabletCellBundle(TTabletCellBundle* cellBundle)
{
    YT_VERIFY(IsTrunk());
    TabletCellBundle_.Assign(cellBundle);
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
    for (auto tablet : Tablets()) {
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
    for (auto tablet : Tablets()) {
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
        for (auto tablet : Tablets()) {
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
            .With("current_mount_transaction_id", transactionId);
    }
}

void TTabletOwnerBase::ValidateTabletStateFixed(TStringBuf message) const
{
    ValidateNoCurrentMountTransaction(message);

    const auto* trunkNode = GetTrunkNode();
    if (auto transactionId = trunkNode->GetLastMountTransactionId()) {
        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::InvalidTabletState, "%v since some tablets are in transient state", message)
            .With("last_mount_transaction_id", transactionId)
            .With("expected_tablet_state", trunkNode->GetExpectedTabletState());
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
            .With("actual_tablet_state", trunkNode->GetActualTabletState())
            .With("expected_tablet_state", trunkNode->GetExpectedTabletState());
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
    const std::vector<i64>& /*trimmedRowCounts*/,
    const std::vector<i64>& /*cumulativeDataWeights*/) const
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
