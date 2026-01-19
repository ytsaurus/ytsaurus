#include "originator_tablet.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

void TOriginatorTablet::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TabletId);
    Persist(context, InheritedCompressedDataSize);
    Persist(context, OriginatorCompressedDataSize);
}

void ToProto(
    NTabletNode::NProto::TOriginatorTablet* protoOriginatorTablet,
    const TOriginatorTablet& originatorTablet)
{
    using NYT::ToProto;

    ToProto(protoOriginatorTablet->mutable_tablet_id(), originatorTablet.TabletId);
    protoOriginatorTablet->set_inherited_compressed_data_size(originatorTablet.InheritedCompressedDataSize);
    protoOriginatorTablet->set_originator_compressed_data_size(originatorTablet.OriginatorCompressedDataSize);
}

void FromProto(
    TOriginatorTablet* originatorTablet,
    const NTabletNode::NProto::TOriginatorTablet& protoOriginatorTablet)
{
    using NYT::FromProto;

    originatorTablet->TabletId = FromProto<TTabletId>(protoOriginatorTablet.tablet_id()),
    originatorTablet->InheritedCompressedDataSize = protoOriginatorTablet.inherited_compressed_data_size(),
    originatorTablet->OriginatorCompressedDataSize = protoOriginatorTablet.originator_compressed_data_size();
}

void Serialize(const TOriginatorTablet& originatorTablet, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("tablet_id").Value(originatorTablet.TabletId)
            .Item("inherited_compressed_data_size").Value(originatorTablet.InheritedCompressedDataSize)
            .Item("originator_compressed_data_size").Value(originatorTablet.OriginatorCompressedDataSize)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
