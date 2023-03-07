#include "config.h"
#include "private.h"
#include "cell_bundle.h"
#include "cell_base.h"

#include <yt/server/master/tablet_server/tablet_action.h>
#include <yt/server/master/tablet_server/tablet_cell.h>
#include <yt/server/master/tablet_server/tablet_cell_bundle.h>
#include <yt/server/master/tablet_server/config.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCellBundle::TCellBundle(TCellBundleId id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
    , CellBalancerConfig_(New<TCellBalancerConfig>())
    , Health_(ETabletCellHealth::Failed)
    , DynamicOptions_(New<TDynamicTabletCellOptions>())
{ }

TString TCellBundle::GetLowercaseObjectName() const
{
    return Format("cell bundle %Qv", Name_);
}

TString TCellBundle::GetCapitalizedObjectName() const
{
    return Format("Cell bundle %Qv", Name_);
}

void TCellBundle::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Acd_);
    Save(context, *Options_);
    Save(context, *DynamicOptions_);
    Save(context, DynamicConfigVersion_);
    Save(context, NodeTagFilter_);
    Save(context, *CellBalancerConfig_);
    Save(context, Health_);
}

void TCellBundle::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Acd_);
    Load(context, *Options_);
    Load(context, *DynamicOptions_);
    Load(context, DynamicConfigVersion_);
    Load(context, NodeTagFilter_);
    // COMPAT(savrus)
    if (context.GetVersion() < EMasterReign::CellServer) {
        Load<THashSet<NTabletServer::TTabletCell*>>(context);
    }
    // COMPAT(savrus)
    if (context.GetVersion() < EMasterReign::CellServer) {
        YT_VERIFY(GetType() == EObjectType::TabletCellBundle);
        auto* tabletCellBundle = this->As<TTabletCellBundle>();
        Load(context, *tabletCellBundle->TabletBalancerConfig());
        CellBalancerConfig()->EnableTabletCellSmoothing = tabletCellBundle->TabletBalancerConfig()->EnableTabletCellSmoothing;
    } else {
        Load(context, *CellBalancerConfig_);
    }
    Load(context, Health_);
    // COMPAT(savrus)
    if (context.GetVersion() < EMasterReign::CellServer) {
        Load<THashSet<NTabletServer::TTabletAction*>>(context);
        Load<int>(context);
    }

    FillProfilingTag();
}

void TCellBundle::SetName(TString name)
{
    Name_ = name;
    FillProfilingTag();
}

TString TCellBundle::GetName() const
{
    return Name_;
}

TDynamicTabletCellOptionsPtr TCellBundle::GetDynamicOptions() const
{
    return DynamicOptions_;
}

void TCellBundle::SetDynamicOptions(TDynamicTabletCellOptionsPtr dynamicOptions)
{
    DynamicOptions_ = std::move(dynamicOptions);
    ++DynamicConfigVersion_;
}

void TCellBundle::FillProfilingTag()
{
    ProfilingTag_ = NProfiling::TProfileManager::Get()->RegisterTag("tablet_cell_bundle", Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
