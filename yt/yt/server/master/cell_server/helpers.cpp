#include "helpers.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellServer {

using namespace NCellarClient;
using namespace NCellarAgent;
using namespace NObjectClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

int GetCellShardIndex(TCellId cellId)
{
    return GetShardIndex<CellShardCount>(cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
