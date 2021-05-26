#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/cell_bundle.h>

#include <yt/yt/core/misc/ref_tracked.h>
#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellBundle
    : public NCellServer::TCellBundle
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TChaosHydraConfigPtr, ChaosOptions);

public:
    explicit TChaosCellBundle(TChaosCellBundleId id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
