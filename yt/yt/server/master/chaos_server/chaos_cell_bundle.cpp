#include "chaos_cell_bundle.h"

#include "chaos_cell.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

TChaosCellBundle::TChaosCellBundle(TChaosCellBundleId id)
    : TCellBundle(id)
    , ChaosOptions_(New<TChaosHydraConfig>())
{ }

void TChaosCellBundle::RemoveMetadataCell(TChaosCell* cell)
{
    std::erase(MetadataCells_, cell);
}

void TChaosCellBundle::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCellBundle>();

    PHOENIX_REGISTER_FIELD(1, ChaosOptions_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(2, MetadataCells_);
}

PHOENIX_DEFINE_TYPE(TChaosCellBundle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
