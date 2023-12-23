#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/cell_bundle.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/profiling/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellBundle
    : public NCellServer::TCellBundle
{
public:
    DEFINE_BYREF_RW_PROPERTY(TChaosHydraConfigPtr, ChaosOptions);

    using TMetadataCells = std::vector<TChaosCell*>;
    DEFINE_BYREF_RW_PROPERTY(TMetadataCells, MetadataCells);

public:
    explicit TChaosCellBundle(TChaosCellBundleId id);

    void RemoveMetadataCell(TChaosCell* cell);

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

DEFINE_MASTER_OBJECT_TYPE(TChaosCellBundle)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
