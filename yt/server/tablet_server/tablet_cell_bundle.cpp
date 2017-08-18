#include "tablet_cell_bundle.h"
#include "tablet_cell.h"

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(const TTabletCellBundleId& id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
{ }

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Acd_);
    Save(context, *Options_);
    Save(context, NodeTagFilter_);
    Save(context, TabletCells_);
    Save(context, EnableTabletBalancer_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, Acd_);
    }
    Load(context, *Options_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        // COMPAT(savrus)
        if (context.GetVersion() >= 600) {
            Load(context, NodeTagFilter_);
        } else {
            if (auto filter = Load<TNullable<TString>>(context)) {
                NodeTagFilter_ = MakeBooleanFormula(*filter);
            }
        }
    }
    // COMAPT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, TabletCells_);
    }
    if (context.GetVersion() >= 614) {
        Load(context, EnableTabletBalancer_);
    }

    //COMPAT(savrus)
    if (context.GetVersion() < 614) {
        if (Attributes_) {
            auto& attributes = Attributes_->Attributes();
            static const TString nodeTagFilterAttributeName("node_tag_filter");
            auto it = attributes.find(nodeTagFilterAttributeName);
            if (it != attributes.end()) {
                attributes.erase(it);
            }
            if (attributes.empty()) {
                Attributes_.reset();
            }
        }
    }
}

void TTabletCellBundle::FillProfilingTag()
{
    ProfilingTag_ = NProfiling::TProfileManager::Get()->RegisterTag("tablet_cell_bundle", Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

