#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/client/api/public.h>

namespace NYP {
namespace NServer {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

void ValidateHostDeviceSpec(const NClient::NApi::NProto::TPodSpec_THostDevice& spec);
void ValidateSysctlProperty(const NClient::NApi::NProto::TPodSpec_TSysctlProperty& spec);

std::vector<std::pair<TString, TString>> BuildPortoProperties(
    NObjects::TNode* node,
    NObjects::TPod* pod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP
