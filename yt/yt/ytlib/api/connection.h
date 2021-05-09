#pragma once

#include "public.h"

#include <yt/yt/client/api/connection.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    NYTree::INodePtr config,
    TConnectionOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
