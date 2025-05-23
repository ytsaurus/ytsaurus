#include "tablet_resources.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

namespace NYT::NTabletServer {

using namespace NYTree;
using namespace NSecurityServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TTabletResources::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, TabletCount_);
    Persist(context, TabletStaticMemory_);
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

void TTabletCellBundleQuota::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cpu_);
    Persist(context, Memory_);
    Persist(context, NetBytes_);
}

void Serialize(const TTabletCellBundleQuota& resources, NYTree::TFluentMap& fluent)
{
    fluent
        .OptionalItem("cpu", resources.GetCpu())
        .OptionalItem("memory", resources.GetMemory())
        .OptionalItem("net_bytes", resources.GetNetBytes());
}

void Serialize(const TTabletCellBundleQuota& resources, NYson::IYsonConsumer* consumer)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    Serialize(resources, fluent);
    fluent
        .EndMap();
}

void Deserialize(TTabletCellBundleQuota& resources, const NYTree::INodePtr& node)
{
    auto getField = [] (const NYTree::IMapNodePtr& mapNode, const std::string& key)
        -> std::optional<i64>
    {
        auto fieldNode = mapNode->FindChild(key);
        if (!fieldNode) {
            return {};
        }

        auto result = fieldNode->AsInt64()->GetValue();
        if (result < 0) {
            THROW_ERROR_EXCEPTION("%Qv cannot be negative, found %v", key, result);
        }
        return result;
    };

    auto map = node->AsMap();

    auto result = TTabletCellBundleQuota()
        .SetCpu(getField(map, "cpu"))
        .SetMemory(getField(map, "memory"))
        .SetNetBytes(getField(map, "net_bytes"));

    resources = result;
}

////////////////////////////////////////////////////////////////////////////////

void TTabletCellBundleResources::Persist(const NCellMaster::TPersistenceContext& context)
{
    TTabletResources::Persist(context);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= EMasterReign::ResourceQuotaAttributeForBundles) {
        TTabletCellBundleQuota::Persist(context);
    }
}

void Serialize(const TTabletCellBundleResources& resources, NYTree::TFluentMap& fluent)
{
    Serialize(static_cast<const TTabletResources&>(resources), fluent);
    Serialize(static_cast<const TTabletCellBundleQuota&>(resources), fluent);
}

void Serialize(const TTabletCellBundleResources& resources, NYson::IYsonConsumer* consumer)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    Serialize(resources, fluent);
    fluent
        .EndMap();
}

void Deserialize(TTabletCellBundleResources& resources, const NYTree::INodePtr& node)
{
    Deserialize(static_cast<TTabletResources&>(resources), node);
    Deserialize(static_cast<TTabletCellBundleQuota&>(resources), node);
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
