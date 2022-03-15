#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteClusterTimestampProvider(
    NApi::NNative::IConnectionPtr nativeConnection,
    NObjectClient::TCellTag clockClusterTag,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
