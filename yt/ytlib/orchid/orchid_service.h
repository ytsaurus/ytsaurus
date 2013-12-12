#pragma once

#include "common.h"
#include "orchid_service_proxy.h"

#include <core/ytree/public.h>
#include <core/rpc/service_detail.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidService
    : public NRpc::TServiceBase
{
public:
    TOrchidService(
        NYTree::INodePtr root,
        IInvokerPtr invoker);

private:
    NYTree::IYPathServicePtr RootService;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

