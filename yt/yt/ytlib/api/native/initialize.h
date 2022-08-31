#include "public.h"

#include "connection.h"

#include <yt/yt/library/auth_server/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

NAuth::IDynamicTvmServicePtr CreateMainConnectionTvmService(
    const NAuth::TTvmServiceConfigPtr& tvmServiceConfig,
    const TConnectionConfigPtr& connectionConfig);

IConnectionPtr CreateMainConnection(
    const NAuth::IDynamicTvmServicePtr& tvmService,
    const TConnectionConfigPtr& config,
    TConnectionOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
