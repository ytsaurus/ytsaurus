#pragma once

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    NCoreDump::ICoreDumperPtr coreDumper,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
