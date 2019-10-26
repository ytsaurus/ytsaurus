#pragma once

#include "public.h"

#include <yp/server/objects/public.h>
#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/misc/public.h>

namespace NYP::NServer::NNodes {

////////////////////////////////////////////////////////////////////////////////

void ValidateHostDeviceSpec(const NClient::NApi::NProto::TPodSpec_THostDevice& spec);
void ValidateSysctlProperty(const NClient::NApi::NProto::TPodSpec_TSysctlProperty& spec);

std::vector<std::pair<TString, TString>> BuildPortoProperties(
    const NClient::NApi::NProto::TResourceSpec_TCpuSpec& cpuSpec,
    const NObjects::NProto::TPodSpecEtc& podSpecEtc,
    const NObjects::NProto::TPodStatusEtc& podStatusEtc);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
