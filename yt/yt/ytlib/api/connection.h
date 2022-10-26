#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/client/api/connection.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    NYTree::INodePtr config,
    TConnectionOptions options = {},
    NAuth::IDynamicTvmServicePtr tvmService = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
