#pragma once

#include <yt/yt/server/lib/tablet_server/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TOriginatorTablet
{
    TTabletId TabletId;
    i64 InheritedCompressedDataSize;
    i64 OriginatorCompressedDataSize;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(
    NTabletNode::NProto::TOriginatorTablet* protoOriginatorTablet,
    const TOriginatorTablet& originatorTablet);

void FromProto(
    TOriginatorTablet* originatorTablet,
    const NTabletNode::NProto::TOriginatorTablet& protoOriginatorTablet);

void Serialize(const TOriginatorTablet& originatorTablet, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
