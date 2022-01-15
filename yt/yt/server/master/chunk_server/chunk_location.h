#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkLocation
    : public NObjectServer::TObject
    , public TRefTracked<TChunkLocation>
{
public:
    using TObject::TObject;

    DEFINE_BYVAL_RW_PROPERTY(TChunkLocationUuid, Uuid)
    DEFINE_BYVAL_RW_PROPERTY(TNode*, Node)
    DEFINE_BYVAL_RW_PROPERTY(EChunkLocationState, State, EChunkLocationState::Offline)
    DEFINE_BYREF_RW_PROPERTY(TMediumPtr, MediumOverride)
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TChunkLocationStatistics, Statistics)

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TChunkLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
