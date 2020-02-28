#pragma once

#include "public.h"

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/security_server/acl.h>

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/tablet_server/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/arithmetic_formula.h>

#include <yt/core/profiling/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellBundle
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TCellBundle>
{
public:
    DECLARE_BYVAL_RW_PROPERTY(TString, Name);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TTabletCellOptionsPtr, Options);
    DECLARE_BYVAL_RW_PROPERTY(TDynamicTabletCellOptionsPtr, DynamicOptions);
    DEFINE_BYVAL_RO_PROPERTY(int, DynamicConfigVersion);
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormula, NodeTagFilter);
    DEFINE_BYREF_RW_PROPERTY(TCellBalancerConfigPtr, CellBalancerConfig);
    DEFINE_BYREF_RW_PROPERTY(ECellHealth, Health, ECellHealth::Good);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TCellBase*>, Cells);

    DEFINE_BYVAL_RO_PROPERTY(NProfiling::TTagId, ProfilingTag);

public:
    explicit TCellBundle(TCellBundleId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void IncreaseActiveTabletActionCount();
    void DecreaseActiveTabletActionCount();

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

private:
    TString Name_;
    TDynamicTabletCellOptionsPtr DynamicOptions_;

    void FillProfilingTag();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
