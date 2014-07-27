#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TCallback<void(TErrorOr<TMutationResponse>)> CreateRpcResponseHandler(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
