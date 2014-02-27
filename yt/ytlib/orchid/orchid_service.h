#pragma once

#include <core/ytree/public.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOrchidService(
    NYTree::INodePtr root,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

