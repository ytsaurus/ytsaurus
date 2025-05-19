#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/gossip_value.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TTabletResources, i64, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TTabletResources, i64, TabletStaticMemory);

public:
    bool operator==(const TTabletResources& other) const = default;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

void Serialize(const TTabletResources& tabletResources, NYTree::TFluentMap& fluent);
void Serialize(const TTabletResources& tabletResources, NYson::IYsonConsumer* consumer);
void Deserialize(TTabletResources& tabletResources, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTabletResources* protoResources, const TTabletResources& resources);
void FromProto(TTabletResources* resources, const NProto::TTabletResources& protoResources);

TTabletResources operator+(TTabletResources lhs, const TTabletResources& rhs);
TTabletResources& operator+=(TTabletResources& lhs, const TTabletResources& rhs);

TTabletResources operator-(TTabletResources lhs, const TTabletResources& rhs);
TTabletResources& operator-=(TTabletResources& lhs, const TTabletResources& rhs);

TTabletResources operator-(const TTabletResources& value);

void FormatValue(TStringBuilderBase* builder, const TTabletResources& resources, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

using TGossipTabletResources = NCellMaster::TGossipValue<TTabletResources>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
