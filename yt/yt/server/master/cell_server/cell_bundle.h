#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/profiling/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellBundleProfilingCounters
{
    NProfiling::TCounter ReplicaModeSwitch;
    NProfiling::TCounter InMemoryMoves;
    NProfiling::TCounter ExtMemoryMoves;
    NProfiling::TCounter TabletMerges;
    NProfiling::TCounter TabletCellMoves;
    NProfiling::TCounter PeerAssignment;

    TEnumIndexedArray<ECellHealth, NProfiling::TGauge> TabletCellCount;

    NProfiling::TProfiler Profiler;

    static TString FormatErrorCode(TErrorCode errorCode);

    THashMap<TErrorCode, NProfiling::TCounter> LeaderReassignment;

    NProfiling::TCounter& GetLeaderReassignment(TErrorCode errorCode);

    THashMap<TErrorCode, NProfiling::TCounter> PeerRevocation;

    NProfiling::TCounter& GetPeerRevocation(TErrorCode errorCode);
};

////////////////////////////////////////////////////////////////////////////////

class TCellBundle
    : public NObjectServer::TObject
    , public TRefTracked<TCellBundle>
{
public:
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TTabletCellOptionsPtr, Options);
    DECLARE_BYVAL_RW_PROPERTY(TDynamicTabletCellOptionsPtr, DynamicOptions);
    DEFINE_BYVAL_RO_PROPERTY(int, DynamicConfigVersion);
    DEFINE_BYREF_RW_PROPERTY(TCellBalancerBootstrapConfigPtr, CellBalancerConfig);
    DEFINE_BYREF_RW_PROPERTY(ECellHealth, Health, ECellHealth::Good);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TCellBase*>, Cells);

    using TAreaMap = THashMap<std::string, TArea*>;
    DEFINE_BYREF_RW_PROPERTY(TAreaMap, Areas);
    DEFINE_BYVAL_RW_PROPERTY(TArea*, DefaultArea);

    DEFINE_BYREF_RW_PROPERTY(TCellBundleProfilingCounters, ProfilingCounters);
    DEFINE_BYVAL_RW_PROPERTY(int, ConfigVersion);

public:
    explicit TCellBundle(TCellBundleId id);

    void SetName(const std::string& name);
    const std::string& GetName() const;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void IncreaseActiveTabletActionCount();
    void DecreaseActiveTabletActionCount();

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

    TArea* GetAreaOrThrow(const std::string& name);

    NCellarClient::ECellarType GetCellarType() const;

private:
    std::string Name_;
    TDynamicTabletCellOptionsPtr DynamicOptions_;

    void InitializeProfilingCounters();
};

DEFINE_MASTER_OBJECT_TYPE(TCellBundle)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
