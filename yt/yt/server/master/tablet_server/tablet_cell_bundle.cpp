#include "config.h"
#include "private.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/object_client/config.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletBalancer;
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
    , TabletBalancerConfig_(New<TMasterBundleTabletBalancerConfig>())
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
        if (IsObjectAlive(cell) && !cell->IsDecommissionStarted() && cell->CellBundle().Get() == this) {
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

TString TTabletCellBundle::GetObjectPath() const
{
    return Format("//sys/tablet_cell_bundles/%v", GetName());
}

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TCellBundle::Save(context);

    using NYT::Save;
    Save(context, *TabletBalancerConfig_);
    Save(context, ResourceLimits_);
    Save(context, ResourceUsage_);
    Save(context, AbcConfig_.operator bool());
    if (AbcConfig_) {
        Save(context, *AbcConfig_);
    }
    Save(context, FolderId_);
    Save(context, BundleControllerTargetConfig_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TCellBundle::Load(context);

    using NYT::Load;

    Load(context, *TabletBalancerConfig_);
    Load(context, ResourceLimits_);
    Load(context, ResourceUsage_);
    if (Load<bool>(context)) {
        AbcConfig_ = New<NObjectClient::TAbcConfig>();
        Load(context, *AbcConfig_);
    }
    Load(context, FolderId_);
    Load(context, BundleControllerTargetConfig_);
}

void TTabletCellBundle::OnProfiling(TTabletCellBundleProfilingCounters* counters)
{
    counters->TabletCountLimit.Update(ResourceLimits_.TabletCount);
    counters->TabletCountUsage.Update(ResourceUsage_.Cluster().TabletCount);
    counters->TabletStaticMemoryLimit.Update(ResourceLimits_.TabletStaticMemory);
    counters->TabletStaticMemoryUsage.Update(ResourceUsage_.Cluster().TabletStaticMemory);
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundleProfilingCounters::TTabletCellBundleProfilingCounters(TString bundleName)
    : BundleName(std::move(bundleName))
{
    auto profiler = TabletServerProfiler
        .WithDefaultDisabled()
        .WithPrefix("/bundle_resources")
        .WithTag("tablet_cell_bundle", BundleName)
        .WithGlobal();

    TabletCountLimit = profiler.Gauge("/tablet_count_limit");
    TabletCountUsage = profiler.Gauge("/tablet_count_usage");
    TabletStaticMemoryLimit = profiler.Gauge("/tablet_static_memory_limit");
    TabletStaticMemoryUsage = profiler.Gauge("/tablet_static_memory_usage");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

