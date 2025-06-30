#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TNetworkProject
{
    ui32 Id;
    bool EnableNat64;
    // It is named after the corresponding method of IInstanceLauncher.
    bool DisableNetwork;

    PHOENIX_DECLARE_TYPE(TNetworkProject, 0x37e08b74);
};

////////////////////////////////////////////////////////////////////////////////

TNetworkProject GetNetworkProject(
    const NApi::NNative::IClientPtr& client,
    const std::string& authenticatedUser,
    const std::string& networkProjectName);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNetworkProject* protoNetworkProject, const TNetworkProject& networkProject);

void FromProto(TNetworkProject* networkProject, const NProto::TNetworkProject& protoNetworkProject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
