#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TCallback<void(const TErrorOr<TMutationResponse>&)> CreateRpcResponseHandler(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
