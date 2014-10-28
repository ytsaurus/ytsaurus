#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TCallback<void(const TErrorOr<TMutationResponse>&)> CreateRpcResponseHandler(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
