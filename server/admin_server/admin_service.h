#pragma once

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/misc/public.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    ICoreDumperPtr coreDumper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
