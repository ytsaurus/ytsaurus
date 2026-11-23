#include "tablet_action.h"

#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TTabletAction::GetLowercaseObjectName() const
{
    return Format("tablet action %v", GetId());
}

std::string TTabletAction::GetCapitalizedObjectName() const
{
    return Format("Tablet action %v", GetId());
}

TYPath TTabletAction::GetObjectPath() const
{
    return Format("//sys/tablet_actions/%v", GetId());
}

void TTabletAction::SaveTabletIds()
{
    if (SavedTabletIds_) {
        return;
    }

    std::vector<TTabletId> tabletIds;
    tabletIds.reserve(Tablets_.size());
    for (auto tablet : Tablets_) {
        tabletIds.push_back(tablet->GetId());
    }

    SavedTabletIds_ = std::move(tabletIds);
}

std::vector<TTabletId> TTabletAction::GetTabletIds() const
{
    if (SavedTabletIds_) {
        return *SavedTabletIds_;
    }

    std::vector<TTabletId> tabletIds;
    tabletIds.reserve(Tablets_.size());
    for (auto tablet : Tablets_) {
        tabletIds.push_back(tablet->GetId());
    }
    return tabletIds;
}

void TTabletAction::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Kind_);
    PHOENIX_REGISTER_FIELD(2, State_);
    PHOENIX_REGISTER_FIELD(3, Tablets_);
    PHOENIX_REGISTER_FIELD(4, TabletCells_);
    PHOENIX_REGISTER_FIELD(5, PivotKeys_);
    PHOENIX_REGISTER_FIELD(6, TabletCount_);
    PHOENIX_REGISTER_FIELD(7, SkipFreezing_);
    PHOENIX_REGISTER_FIELD(8, Freeze_);
    PHOENIX_REGISTER_FIELD(9, Error_);
    PHOENIX_REGISTER_FIELD(10, CorrelationId_);
    PHOENIX_REGISTER_FIELD(11, ExpirationTime_);
    PHOENIX_REGISTER_FIELD(12, ExpirationTimeout_);
    // COMPAT(atalmenev)
    PHOENIX_REGISTER_FIELD(13, InplaceReshard_,
        .SinceVersion(EMasterReign::InplaceReshard));
    // COMPAT(atalmenev)
    PHOENIX_REGISTER_FIELD(14, TabletMountRevisions_,
        .SinceVersion(EMasterReign::ReshardRedirectionHint));
    PHOENIX_REGISTER_FIELD(15, TabletCellBundle_);
    PHOENIX_REGISTER_FIELD(16, SavedTabletIds_);
}

PHOENIX_DEFINE_TYPE(TTabletAction);

bool TTabletAction::IsFinished() const
{
    return State_ == ETabletActionState::Completed || State_ == ETabletActionState::Failed;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTabletAction& action, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{ActionId: %v, State: %v, Kind: %v, SkipFreezing: %v, Freeze: %v, TabletCount: %v, Tablets: %v, "
        "Cells: %v, PivotKeys: %v, TabletBalancerCorrelationId: %v, ExpirationTime: %v, ExpirationTimeout: %v, "
        "InplaceReshard: %v, TabletMountRevisions: %v, TableId: %v, Bundle: %v}",
        action.GetId(),
        action.GetState(),
        action.GetKind(),
        action.GetSkipFreezing(),
        action.GetFreeze(),
        action.GetTabletCount(),
        MakeFormattableView(action.Tablets(), TObjectIdFormatter()),
        MakeFormattableView(action.TabletCells(), TObjectIdFormatter()),
        action.PivotKeys(),
        action.GetCorrelationId(),
        action.GetExpirationTime(),
        action.GetExpirationTimeout(),
        action.IsInplaceReshard(),
        action.TabletMountRevisions(),
        action.Tablets()[0]->GetOwner()->GetId(),
        action.GetTabletCellBundle()->GetName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
