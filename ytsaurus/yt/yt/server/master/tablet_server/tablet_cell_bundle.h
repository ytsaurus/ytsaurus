#pragma once

#include "public.h"
#include "tablet_resources.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/cell_bundle.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/core/misc/ref_tracked.h>
#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellBundleProfilingCounters
{
    TTabletCellBundleProfilingCounters(TString bundleName);

    NProfiling::TGauge TabletCountLimit;
    NProfiling::TGauge TabletCountUsage;
    NProfiling::TGauge TabletStaticMemoryLimit;
    NProfiling::TGauge TabletStaticMemoryUsage;

    TString BundleName;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundle
    : public NCellServer::TCellBundle
{
public:
    DEFINE_BYREF_RW_PROPERTY(NTabletBalancer::TMasterBundleTabletBalancerConfigPtr, TabletBalancerConfig);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletAction*>, TabletActions);
    DEFINE_BYVAL_RO_PROPERTY(int, ActiveTabletActionCount);

    DEFINE_BYREF_RW_PROPERTY(TTabletResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(TGossipTabletResources, ResourceUsage);

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TAbcConfigPtr, AbcConfig);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, FolderId);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYson::TYsonString>, BundleControllerTargetConfig);

public:
    using NCellServer::TCellBundle::TCellBundle;
    explicit TTabletCellBundle(TTabletCellBundleId id);

    void IncreaseActiveTabletActionCount();
    void DecreaseActiveTabletActionCount();

    std::vector<const TTabletCell*> GetAliveCells() const;

    void ValidateResourceUsageIncrease(const TTabletResources& delta) const;
    void UpdateResourceUsage(TTabletResources delta);
    void RecomputeClusterResourceUsage();

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    void OnProfiling(TTabletCellBundleProfilingCounters* counters);
};

DEFINE_MASTER_OBJECT_TYPE(TTabletCellBundle)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
