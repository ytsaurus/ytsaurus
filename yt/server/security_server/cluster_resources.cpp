#include "stdafx.h"
#include "cluster_resources.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/yson_serializable.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace(0)
    , NodeCount(0)
{ }

TClusterResources::TClusterResources(i64 diskSpace, int nodeCount)
    : DiskSpace(diskSpace)
    , NodeCount(nodeCount)
{ }

////////////////////////////////////////////////////////////////////////////////

//! A serialization-enabling wrapper around TClusterResources.
struct TSerializableClusterAttributes
    : public TClusterResources
    , public TYsonSerializableLite
{
    TSerializableClusterAttributes(const TClusterResources& other = ZeroClusterResources())
        : TClusterResources(other)
    {
        Register("disk_space", DiskSpace)
            .GreaterThanOrEqual(0);
        Register("node_count", NodeCount)
            .GreaterThanOrEqual(0);
    }
};

void Serialize(const TClusterResources& resources, IYsonConsumer* consumer)
{
    TSerializableClusterAttributes wrapper(resources);
    Serialize(static_cast<const TYsonSerializableLite&>(wrapper), consumer);
}

void Deserialize(TClusterResources& value, INodePtr node)
{
    TSerializableClusterAttributes wrapper;
    Deserialize(static_cast<TYsonSerializableLite&>(wrapper), node);
    // TODO(babenko): we shouldn't be concerned with manual validation here
    wrapper.Validate();
    value = static_cast<TClusterResources&>(wrapper);
}

////////////////////////////////////////////////////////////////////////////////

void Save(const NCellMaster::TSaveContext& context, const TClusterResources& resources)
{
    auto* output = context.GetOutput();
    ::Save(output, resources.DiskSpace);
    ::Save(output, resources.NodeCount);
}

void Load(const NCellMaster::TLoadContext& context, TClusterResources& resources)
{
    auto* input = context.GetInput();
    ::Load(input, resources.DiskSpace);
    // COMPAT(babenko)
    if (context.GetVersion() >= 8) {
        ::Load(input, resources.NodeCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

const TClusterResources& ZeroClusterResources()
{
    static TClusterResources zero;
    return zero;
}

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs)
{
    lhs.DiskSpace += rhs.DiskSpace;
    lhs.NodeCount += rhs.NodeCount;
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
    lhs.NodeCount -= rhs.NodeCount;
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
    lhs.NodeCount *= rhs;
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
    result.NodeCount = -resources.NodeCount;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

