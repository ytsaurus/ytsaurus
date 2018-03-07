#include "config.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(const TTabletCellBundleId& id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
    , TabletBalancerConfig_(New<TTabletBalancerConfig>())
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
    Save(context, *TabletBalancerConfig_);
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
    // COMPAT(savrus)
    if (context.GetVersion() >= 625) {
        Load(context, *Options_);
    } else {
        auto str = NYT::Load<TYsonString>(context);
        auto node = ConvertTo<INodePtr>(str);
        node->AsMap()->AddChild(ConvertTo<INodePtr>(DefaultStoreAccountName), "changelog_account");
        node->AsMap()->AddChild(ConvertTo<INodePtr>(DefaultStoreAccountName), "snapshot_account");
        Options_->Load(node);
    }
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
    // COMPAT(savrus)
    if (context.GetVersion() >= 624) {
        Load(context, *TabletBalancerConfig_);
    } else if (context.GetVersion() >= 614) {
        bool enableTabletBalancer;
        Load(context, enableTabletBalancer);
        TabletBalancerConfig_->EnableInMemoryBalancer = enableTabletBalancer;
        TabletBalancerConfig_->EnableTabletSizeBalancer = enableTabletBalancer;
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

void TTabletCellBundle::FillProfilingTag()
{
    ProfilingTag_ = NProfiling::TProfileManager::Get()->RegisterTag("tablet_cell_bundle", Name_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

