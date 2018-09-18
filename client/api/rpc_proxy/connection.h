#pragma once

#include "public.h"

#include <yt/client/api/public.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr CreateConnection(
    TConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
