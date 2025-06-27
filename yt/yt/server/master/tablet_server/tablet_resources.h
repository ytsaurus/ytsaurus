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

    void Persist(const NCellMaster::TPersistenceContext& context);
};

void Serialize(const TTabletResources& tabletResources, NYTree::TFluentMap& fluent);
void Serialize(const TTabletResources& tabletResources, NYson::IYsonConsumer* consumer);
void Deserialize(TTabletResources& tabletResources, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleQuota
{
public:
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TTabletCellBundleQuota, std::optional<i64>, Cpu);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TTabletCellBundleQuota, std::optional<i64>, Memory);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TTabletCellBundleQuota, std::optional<i64>, NetBytes);

public:
    void Persist(const NCellMaster::TPersistenceContext& context);
};

void Serialize(const TTabletCellBundleQuota& resources, NYTree::TFluentMap& fluent);
void Serialize(const TTabletCellBundleQuota& resources, NYson::IYsonConsumer* consumer);
void Deserialize(TTabletCellBundleQuota& resources, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundleResources
    : public TTabletResources
    , public TTabletCellBundleQuota
{
public:
    void Persist(const NCellMaster::TPersistenceContext& context);
};

void Serialize(const TTabletCellBundleResources& resources, NYTree::TFluentMap& fluent);
void Serialize(const TTabletCellBundleResources& resources, NYson::IYsonConsumer* consumer);
void Deserialize(TTabletCellBundleResources& resources, const NYTree::INodePtr& node);

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
