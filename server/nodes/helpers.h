#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/client/api/proto/dynamic_attributes.pb.h>

namespace NYP::NServer::NNodes {

////////////////////////////////////////////////////////////////////////////////

void SchedulePodDynamicAttributesLoad(NObjects::TPod* pod);
NClient::NApi::NProto::TDynamicAttributes BuildPodDynamicAttributes(NObjects::TPod* pod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
