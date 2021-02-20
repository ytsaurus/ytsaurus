#include "config.h"
#include "private.h"
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
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(TTabletCellBundleId id)
    : TCellBundle(id)
    , TabletBalancerConfig_(New<TTabletBalancerConfig>())
{ }

void TTabletCellBundle::IncreaseActiveTabletActionCount()
{
    ++ActiveTabletActionCount_;
}

void TTabletCellBundle::DecreaseActiveTabletActionCount()
{
    YT_LOG_ERROR_UNLESS(ActiveTabletActionCount_ > 0,
        "Attempting to decrease non-positive ActiveTabletActionCount "
        "(ActiveTabletActionCount: %v, Bundle: %v)",
        ActiveTabletActionCount_,
        GetName());
    --ActiveTabletActionCount_;
}

std::vector<const TTabletCell*> TTabletCellBundle::GetAliveCells() const
{
    std::vector<const TTabletCell*> cells;
    for (const auto* cell : Cells()) {
        if (IsObjectAlive(cell) && !cell->IsDecommissionStarted() && cell->GetCellBundle() == this) {
            YT_VERIFY(cell->GetType() == EObjectType::TabletCell);
            cells.push_back(cell->As<TTabletCell>());
        }
    }
    return cells;
}

void TTabletCellBundle::ValidateResourceUsageIncrease(const TTabletResources& delta) const
{
    const auto& usage = ResourceUsage_.Cluster();
    const auto& limits = ResourceLimits_;

    YT_LOG_DEBUG("QWFP Validate resource usage increase (Usage: %v, Limits: %v, Delta: %v)",
        usage,
        limits,
        delta);

    auto validate = [&] (TStringBuf resourceName, auto TTabletResources::* resource) {
        if (delta.*resource > 0 &&
            usage.*resource + delta.*resource > limits.*resource)
        {
            THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::BundleResourceLimitExceeded,
                "Tablet cell bundle %Qv is over %v limit",
                GetName(),
                resourceName)
                << TErrorAttribute("increase", delta.*resource)
                << TErrorAttribute("usage", usage.*resource)
                << TErrorAttribute("limit", limits.*resource);
        }
    };

    validate("tablet count", &TTabletResources::TabletCount);
    validate("tablet static memory", &TTabletResources::TabletStaticMemory);
}

void TTabletCellBundle::UpdateResourceUsage(TTabletResources delta)
{
    YT_LOG_DEBUG("QWFP Updated resource usage (Delta: %v)", delta);
    ResourceUsage_.Local() += delta;
    if (ResourceUsage_.GetLocalPtr() != &ResourceUsage_.Cluster()) {
        ResourceUsage_.Cluster() += delta;
    }
}

void TTabletCellBundle::RecomputeClusterResourceUsage()
{
    ResourceUsage_.Cluster() = {};
    for (const auto& [cellTag, resourceUsage] : ResourceUsage_.Multicell()) {
        ResourceUsage_.Cluster() += resourceUsage;
    }
}

TString TTabletCellBundle::GetLowercaseObjectName() const
{
    return Format("tablet cell bundle %Qv", GetName());
}

TString TTabletCellBundle::GetCapitalizedObjectName() const
{
    return Format("Tablet cell bundle %Qv", GetName());
}

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TCellBundle::Save(context);

    using NYT::Save;
    Save(context, *TabletBalancerConfig_);
    Save(context, ResourceLimits_);
    Save(context, ResourceUsage_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TCellBundle::Load(context);

    using NYT::Load;

    Load(context, *TabletBalancerConfig_);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= EMasterReign::BundleQuotas) {
        Load(context, ResourceLimits_);
        Load(context, ResourceUsage_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

