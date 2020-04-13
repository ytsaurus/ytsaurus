#include "public.h"

#include <yt/core/ypath/token.h>

namespace NYT::NTabletClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPath GetCypressClustersPath()
{
    return "//sys/clusters";
}

TYPath GetCypressClusterPath(const TString& name)
{
    return GetCypressClustersPath() + "/" + ToYPathLiteral(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

