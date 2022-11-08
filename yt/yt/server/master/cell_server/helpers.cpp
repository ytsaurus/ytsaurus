#include "helpers.h"

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellServer {

using namespace NCellarClient;
using namespace NObjectClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

int GetCellShardIndex(TCellId cellId)
{
    return TDirectObjectIdHash()(cellId) % CellShardCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
