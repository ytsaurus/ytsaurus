#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_server/cell_bundle.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellBundle
    : public NCellServer::TCellBundle
{
public:
    DEFINE_BYREF_RW_PROPERTY(TChaosHydraConfigPtr, ChaosOptions);

    using TMetadataCells = std::vector<TChaosCellRawPtr>;
    DEFINE_BYREF_RW_PROPERTY(TMetadataCells, MetadataCells);

public:
    explicit TChaosCellBundle(TChaosCellBundleId id);

    void RemoveMetadataCell(TChaosCell* cell);

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChaosCellBundle, 0xf589a066);
};

DEFINE_MASTER_OBJECT_TYPE(TChaosCellBundle)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
