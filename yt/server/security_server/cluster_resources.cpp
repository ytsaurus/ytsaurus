#include "stdafx.h"
#include "cluster_resources.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace(0)
{ }

TClusterResources TClusterResources::FromDiskSpace(i64 diskSpace)
{
    TClusterResources result;
    result.DiskSpace = diskSpace;
    return result;
}

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

void Save(TOutputStream* output, const TClusterResources& resources)
{
    ::Save(output, resources.DiskSpace);
}

void Load(TInputStream* input, TClusterResources& resources)
{
    ::Load(input, resources.DiskSpace);
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

