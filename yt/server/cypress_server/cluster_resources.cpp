#include "stdafx.h"
#include "cluster_resources.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace(0)
{ }

TClusterResources::TClusterResources(i64 diskSpace)
    : DiskSpace(diskSpace)
{ }

void Serialize(const TClusterResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("disk_space").Scalar(resources.DiskSpace)
        .EndMap();
}

const TClusterResources& ZeroClusterResources()
{
    static TClusterResources zero;
    return zero;
}

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs)
{
    lhs.DiskSpace += rhs.DiskSpace;
    return lhs;
}

TClusterResources operator+ (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

