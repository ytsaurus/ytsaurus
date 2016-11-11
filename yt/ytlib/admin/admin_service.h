#pragma once

#include <yt/core/actions/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(const IInvokerPtr& invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAdmin
} // namespace NYT
