#include "stdafx.h"
#include "cluster_resources.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace(0)
{ }

TClusterResources::TClusterResources(i64 diskSpace)
    : DiskSpace(diskSpace)
{ }

void Serialize(const TClusterResources& resources, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("disk_space").Scalar(resources.DiskSpace)
        .EndMap();
}

void Save(TOutputStream* output, const TClusterResources& resources)
{
    ::Save(output, resources.DiskSpace);
}

void Load(TInputStream* input, TClusterResources& resources)
{
    ::Load(input, resources.DiskSpace);
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

TClusterResources operator + (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs)
{
    lhs.DiskSpace -= rhs.DiskSpace;
    return lhs;
}

TClusterResources operator - (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs)
{
    lhs.DiskSpace *= rhs;
    return lhs;
}

TClusterResources operator * (const TClusterResources& lhs, i64 rhs)
{
    auto result = lhs;
    result *= rhs;
    return result;
}

TClusterResources operator -  (const TClusterResources& resources)
{
    TClusterResources result;
    result.DiskSpace = -resources.DiskSpace;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

