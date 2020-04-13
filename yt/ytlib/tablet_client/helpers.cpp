#include "public.h"

#include <yt/core/ypath/token.h>

namespace NYT::NTabletClient {

using namespace NYPath;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TYPath GetCypressClustersPath()
{
    return "//sys/clusters";
}

TYPath GetCypressClusterPath(const TString& name)
{
    return GetCypressClustersPath() + "/" + ToYPathLiteral(name);
}

bool IsChunkTabletStoreType(NObjectClient::EObjectType type)
{
    return type == EObjectType::Chunk ||
        type == EObjectType::ErasureChunk;
}

bool IsDynamicTabletStoreType(NObjectClient::EObjectType type)
{
    return type == EObjectType::SortedDynamicTabletStore ||
        type == EObjectType::OrderedDynamicTabletStore;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

