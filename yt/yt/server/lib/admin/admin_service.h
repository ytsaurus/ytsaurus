#pragma once

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/misc/public.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    ICoreDumperPtr coreDumper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
