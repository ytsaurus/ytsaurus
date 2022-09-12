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
    return CreateConnection(config, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
