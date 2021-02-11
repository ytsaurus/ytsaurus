#include "tablet_resources.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NTabletServer {

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

////////////////////////////////////////////////////////////////////////////////

TSerializableTabletResources::TSerializableTabletResources(const TTabletResources& resources)
    : TTabletResources(resources)
{
    Initialize();

    static_cast<TTabletResources&>(*this) = resources;
}

TSerializableTabletResources::TSerializableTabletResources()
{
    Initialize();
}

void TSerializableTabletResources::Initialize()
{
    RegisterParameter("tablet_count", TabletCount)
        .Default(0);

    RegisterParameter("tablet_static_memory", TabletStaticMemory)
        .Default(0);
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

bool operator==(const TTabletResources& lhs, const TTabletResources& rhs)
{
    return lhs.TabletCount == rhs.TabletCount &&
        lhs.TabletStaticMemory == rhs.TabletStaticMemory;
}

bool operator!=(const TTabletResources& lhs, const TTabletResources& rhs)
{
    return !(lhs == rhs);
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
