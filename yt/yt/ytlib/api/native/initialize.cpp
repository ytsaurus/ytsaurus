#include "initialize.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/tvm_service.h>

namespace NYT::NApi::NNative {

using namespace NAuth;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NAuth::IDynamicTvmServicePtr CreateMainConnectionTvmService(
    const NAuth::TTvmServiceConfigPtr& tvmServiceConfig,
    const TConnectionConfigPtr& connectionConfig)
{
    if (!connectionConfig->TvmId) {
        return nullptr;
    }

    auto appliedTvmServiceConfig = CloneYsonSerializable(tvmServiceConfig);
    appliedTvmServiceConfig->ClientEnableServiceTicketFetching = true;
    appliedTvmServiceConfig->ClientSelfId = *connectionConfig->TvmId;
    YT_VERIFY(appliedTvmServiceConfig->ClientDstMap.emplace("self", *connectionConfig->TvmId).second);

    return CreateDynamicTvmService(appliedTvmServiceConfig);
}

IConnectionPtr CreateMainConnection(
    const NAuth::IDynamicTvmServicePtr& tvmService,
    const TConnectionConfigPtr& config,
    TConnectionOptions options)
{
    YT_VERIFY(!options.TvmService);
    options.TvmService = tvmService;

    auto connection = CreateConnection(config, options);

    connection->GetClusterDirectory()->SubscribeOnClusterUpdated(
        BIND_NO_PROPAGATE([tvmService] (const TString& name, INodePtr nativeConnectionConfig) {
            static const auto& Logger = TvmSynchronizerLogger;

            NNative::TConnectionConfigPtr config;
            try {
                config = ConvertTo<NNative::TConnectionConfigPtr>(nativeConnectionConfig);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Cannot update cluster TVM ids because of invalid connection config (Name: %v)", name);
            }

            if (tvmService && config->TvmId) {
                YT_LOG_INFO("Adding cluster service ticket to TVM client (Name: %v, TvmId: %v)", name, *config->TvmId);
                tvmService->AddDestinationServiceIds({*config->TvmId});
            }
        }));

    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
