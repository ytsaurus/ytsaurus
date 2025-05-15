#include "tablet_resources.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

namespace NYT::NTabletServer {

using namespace NYTree;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

void TTabletResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
}

void TTabletResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
}

void Serialize(const TTabletResources& tabletResources, NYTree::TFluentMap& fluent)
{
    fluent
        .Item("tablet_count").Value(tabletResources.GetTabletCount())
        .Item("tablet_static_memory").Value(tabletResources.GetTabletStaticMemory());
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
    protoResources->set_tablet_count(resources.GetTabletCount());
    protoResources->set_tablet_static_memory(resources.GetTabletStaticMemory());
}

void FromProto(TTabletResources* resources, const NProto::TTabletResources& protoResources)
{
    resources->SetTabletCount(protoResources.tablet_count());
    resources->SetTabletStaticMemory(protoResources.tablet_static_memory());
}

TTabletResources operator+(TTabletResources lhs, const TTabletResources& rhs)
{
    return lhs += rhs;
}

TTabletResources& operator+=(TTabletResources& lhs, const TTabletResources& rhs)
{
    lhs.SetTabletCount(lhs.GetTabletCount() + rhs.GetTabletCount());
    lhs.SetTabletStaticMemory(lhs.GetTabletStaticMemory() + rhs.GetTabletStaticMemory());
    return lhs;
}

TTabletResources operator-(TTabletResources lhs, const TTabletResources& rhs)
{
    return lhs -= rhs;
}

TTabletResources& operator-=(TTabletResources& lhs, const TTabletResources& rhs)
{
    lhs.SetTabletCount(lhs.GetTabletCount() - rhs.GetTabletCount());
    lhs.SetTabletStaticMemory(lhs.GetTabletStaticMemory() - rhs.GetTabletStaticMemory());
    return lhs;
}

TTabletResources operator-(const TTabletResources& value)
{
    return TTabletResources()
        .SetTabletCount(-value.GetTabletCount())
        .SetTabletStaticMemory(-value.GetTabletStaticMemory());
}

void FormatValue(TStringBuilderBase* builder, const TTabletResources& resources, TStringBuf /*spec*/)
{
    builder->AppendFormat("{TabletCount: %v, TabletStaticMemory: %v}",
        resources.GetTabletCount(),
        resources.GetTabletStaticMemory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
