#pragma once

#include "public.h"

#include <yt/server/master/cell_master/gossip_value.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletResources
{
public:
    i64 TabletCount = 0;
    i64 TabletStaticMemory = 0;

    TTabletResources&& SetTabletCount(i64 tabletCount) &&;
    TTabletResources&& SetTabletStaticMemory(i64 tabletStaticMemory) &&;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

class TSerializableTabletResources
    : public virtual NYTree::TYsonSerializable
    , public TTabletResources
{
public:
    explicit TSerializableTabletResources(const TTabletResources& resources);

    TSerializableTabletResources();

private:
    void Initialize();
};

DEFINE_REFCOUNTED_TYPE(TSerializableTabletResources)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTabletResources* protoResources, const TTabletResources& resources);
void FromProto(TTabletResources* resources, const NProto::TTabletResources& protoResources);

bool operator==(const TTabletResources& lhs, const TTabletResources& rhs);
bool operator!=(const TTabletResources& lhs, const TTabletResources& rhs);

TTabletResources operator+(TTabletResources lhs, const TTabletResources& rhs);
TTabletResources& operator+=(TTabletResources& lhs, const TTabletResources& rhs);

TTabletResources operator-(TTabletResources lhs, const TTabletResources& rhs);
TTabletResources& operator-=(TTabletResources& lhs, const TTabletResources& rhs);

TTabletResources operator-(const TTabletResources& value);

void FormatValue(TStringBuilderBase* builder, const TTabletResources& resources, TStringBuf /*format*/);
TString ToString(const TTabletResources& resources);

////////////////////////////////////////////////////////////////////////////////

using TGossipTabletResources = NCellMaster::TGossipValue<TTabletResources>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
