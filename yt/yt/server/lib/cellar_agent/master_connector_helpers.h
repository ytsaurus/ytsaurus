#pragma once

#include "public.h"

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

void AddCellarInfoToHeartbeatRequest(
    NCellarClient::ECellarType cellarType,
    const ICellarPtr& cellar,
    bool readOnly,
    NCellarNodeTrackerClient::NProto::TReqCellarHeartbeat* request);

void UpdateCellarFromHeartbeatResponse(
    NCellarClient::ECellarType cellarType,
    const ICellarPtr& cellar,
    const NCellarNodeTrackerClient::NProto::TRspCellarHeartbeat& response);

TError UpdateSolomonTags(
    const ICellarManagerPtr& cellarManager,
    NCellarClient::ECellarType cellarType,
    const TString& tagName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
