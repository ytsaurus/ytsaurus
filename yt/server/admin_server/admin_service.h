#pragma once

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/misc/public.h>

namespace NYT {
namespace NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    TCoreDumperPtr coreDumper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAdmin
} // namespace NYT
