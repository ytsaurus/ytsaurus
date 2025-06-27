#include "network_project.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NControllerAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TNetworkProject GetNetworkProject(
    const NApi::NNative::IClientPtr& client,
    const std::string& authenticatedUser,
    const std::string& networkProject)
{
    const auto networkProjectPath = "//sys/network_projects/" + ToYPathLiteral(networkProject);
    auto checkPermissionRsp = WaitFor(client->CheckPermission(authenticatedUser, networkProjectPath, EPermission::Use))
        .ValueOrThrow();
    if (checkPermissionRsp.Action == NSecurityClient::ESecurityAction::Deny) {
        THROW_ERROR_EXCEPTION("User %Qv is not allowed to use network project %Qv",
            authenticatedUser,
            networkProject);
    }

    TGetNodeOptions options{
        .Attributes = TAttributeFilter({"project_id", "enable_nat64", "disable_network"})
    };
    auto result = WaitFor(client->GetNode(networkProjectPath, options))
        .ValueOrThrow();

    auto node = ConvertToNode(result);
    const auto& attributes = node->Attributes();

    return TNetworkProject{
        .Id = attributes.Get<ui32>("project_id"),
        .EnableNat64 = attributes.Get<bool>("enable_nat64", false),
        .DisableNetwork = attributes.Get<bool>("disable_network", false),
    };
}

void TNetworkProject::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Id);
    PHOENIX_REGISTER_FIELD(2, EnableNat64);
    PHOENIX_REGISTER_FIELD(3, DisableNetwork);
}

PHOENIX_DEFINE_TYPE(TNetworkProject);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNetworkProject* protoNetworkProject, const TNetworkProject& networkProject)
{
    protoNetworkProject->set_id(networkProject.Id);
    protoNetworkProject->set_enable_nat64(networkProject.EnableNat64);
    protoNetworkProject->set_disable_network(networkProject.DisableNetwork);
}

void FromProto(TNetworkProject* networkProject, const NProto::TNetworkProject& protoNetworkProject)
{
    networkProject->Id = protoNetworkProject.id();
    networkProject->EnableNat64 = protoNetworkProject.enable_nat64();
    networkProject->DisableNetwork = protoNetworkProject.disable_network();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
