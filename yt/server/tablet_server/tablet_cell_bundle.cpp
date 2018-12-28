#include "config.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(TTabletCellBundleId id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
    , TabletBalancerConfig_(New<TTabletBalancerConfig>())
    , DynamicOptions_(New<TDynamicTabletCellOptions>())
{ }

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Acd_);
    Save(context, *Options_);
    Save(context, *DynamicOptions_);
    Save(context, DynamicConfigVersion_);
    Save(context, NodeTagFilter_);
    Save(context, TabletCells_);
    Save(context, *TabletBalancerConfig_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Acd_);
    Load(context, *Options_);
    // COMPAT(savrus)
    if (context.GetVersion() >= 716) {
        Load(context, *DynamicOptions_);
        Load(context, DynamicConfigVersion_);
    }
    Load(context, NodeTagFilter_);
    Load(context, TabletCells_);
    Load(context, *TabletBalancerConfig_);

    FillProfilingTag();
}

void TTabletCellBundle::SetName(TString name)
{
    Name_ = name;
    FillProfilingTag();
}

TString TTabletCellBundle::GetName() const
{
    return Name_;
}

TDynamicTabletCellOptionsPtr TTabletCellBundle::GetDynamicOptions() const
{
    return DynamicOptions_;
}

void TTabletCellBundle::SetDynamicOptions(TDynamicTabletCellOptionsPtr dynamicOptions)
{
    DynamicOptions_ = std::move(dynamicOptions);
    ++DynamicConfigVersion_;
}

void TTabletCellBundle::FillProfilingTag()
{
    ProfilingTag_ = NProfiling::TProfileManager::Get()->RegisterTag("tablet_cell_bundle", Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

