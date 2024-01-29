#include "tablet_resources.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

namespace NYT::NTabletServer {

using namespace NYTree;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TTabletResources&& TTabletResources::SetTabletCount(i64 tabletCount) &&
{
    TabletCount = tabletCount;
    return std::move(*this);
}

TTabletResources&& TTabletResources::SetTabletStaticMemory(i64 tabletStaticMemory) &&
{
    TabletStaticMemory = tabletStaticMemory;
    return std::move(*this);
}

void TTabletResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletCount);
    Save(context, TabletStaticMemory);
}

void TTabletResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
}

void Serialize(const TTabletResources& tabletResources, NYTree::TFluentMap& fluent)
{
    fluent
        .Item("tablet_count").Value(tabletResources.TabletCount)
        .Item("tablet_static_memory").Value(tabletResources.TabletStaticMemory);
}

void Serialize(const TTabletResources& tabletResources, NYson::IYsonConsumer* consumer)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    Serialize(tabletResources, fluent);
    fluent
        .EndMap();
}

void Deserialize(TTabletResources& tabletResources, const NYTree::INodePtr& node)
{
    auto map = node->AsMap();

    auto result = TTabletResources()
        .SetTabletCount(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_count"))
        .SetTabletStaticMemory(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_static_memory"));

    tabletResources = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTabletResources* protoResources, const TTabletResources& resources)
{
    protoResources->set_tablet_count(resources.TabletCount);
    protoResources->set_tablet_static_memory(resources.TabletStaticMemory);
}

void FromProto(TTabletResources* resources, const NProto::TTabletResources& protoResources)
{
    resources->TabletCount = protoResources.tablet_count();
    resources->TabletStaticMemory = protoResources.tablet_static_memory();
}

TTabletResources operator+(TTabletResources lhs, const TTabletResources& rhs)
{
    return lhs += rhs;
}

TTabletResources& operator+=(TTabletResources& lhs, const TTabletResources& rhs)
{
    lhs.TabletCount += rhs.TabletCount;
    lhs.TabletStaticMemory += rhs.TabletStaticMemory;
    return lhs;
}

TTabletResources operator-(TTabletResources lhs, const TTabletResources& rhs)
{
    return lhs -= rhs;
}

TTabletResources& operator-=(TTabletResources& lhs, const TTabletResources& rhs)
{
    lhs.TabletCount -= rhs.TabletCount;
    lhs.TabletStaticMemory -= rhs.TabletStaticMemory;
    return lhs;
}

TTabletResources operator-(const TTabletResources& value)
{
    return TTabletResources()
        .SetTabletCount(-value.TabletCount)
        .SetTabletStaticMemory(-value.TabletStaticMemory);
}

void FormatValue(TStringBuilderBase* builder, const TTabletResources& resources, TStringBuf /*format*/)
{
    builder->AppendFormat("{TabletCount: %v, TabletStaticMemory: %v}",
        resources.TabletCount,
        resources.TabletStaticMemory);
}

TString ToString(const TTabletResources& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
