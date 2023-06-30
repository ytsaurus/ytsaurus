#include "helpers.h"

#include "connection.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/auth/native_authenticator.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NAuth;
using namespace NRpc;
using namespace NYTree;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool IsValidSourceTvmId(const IConnectionPtr& connection, TTvmId tvmId)
{
    return tvmId == connection->GetConfig()->TvmId || connection->GetClusterDirectory()->HasTvmId(tvmId);
}

IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection)
{
    return NAuth::CreateNativeAuthenticator([connection] (TTvmId tvmId) {
        return IsValidSourceTvmId(connection, tvmId);
    });
}

////////////////////////////////////////////////////////////////////////////////

void SetupClusterConnectionDynamicConfigUpdate(
    const IConnectionPtr& connection,
    EClusterConnectionDynamicConfigPolicy policy,
    const INodePtr& staticClusterConnectionNode,
    const TLogger logger)
{
    auto Logger = logger;
    if (policy == EClusterConnectionDynamicConfigPolicy::FromStaticConfig) {
        return;
    }

    YT_LOG_INFO(
        "Setting up cluster connection dynamic config update (Policy: %v, Cluster: %v)",
        policy,
        connection->GetClusterName());

    connection->GetClusterDirectory()->SubscribeOnClusterUpdated(BIND([=] (const TString& clusterName, const INodePtr& configNode) {
        if (clusterName != connection->GetClusterName()) {
            YT_LOG_DEBUG(
                "Skipping cluster directory update for unrelated cluster (UpdatedCluster: %v)",
                clusterName);
            return;
        }

        auto dynamicConfigNode = configNode;

        YT_LOG_DEBUG(
            "Applying cluster connection update from cluster directory (DynamicConfig: %v)",
            ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());

        if (policy == EClusterConnectionDynamicConfigPolicy::FromClusterDirectoryWithStaticPatch) {
            dynamicConfigNode = PatchNode(dynamicConfigNode, staticClusterConnectionNode);
            YT_LOG_DEBUG(
                "Patching cluster connection dynamic config with static config (DynamicConfig: %v)",
                ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());
        }

        TConnectionDynamicConfigPtr dynamicConfig;
        try {
            dynamicConfig = ConvertTo<TConnectionDynamicConfigPtr>(dynamicConfigNode);
            connection->Reconfigure(dynamicConfig);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Failed to apply cluster connection dynamic config, ignoring update");
            return;
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
