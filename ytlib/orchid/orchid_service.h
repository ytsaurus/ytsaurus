#pragma once

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOrchidService(
    NYTree::INodePtr root,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

