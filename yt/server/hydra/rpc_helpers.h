#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TCallback<void(const TMutationResponse&)> CreateRpcSuccessHandler(NRpc::IServiceContextPtr context);
TCallback<void(const TError&)> CreateRpcErrorHandler(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
