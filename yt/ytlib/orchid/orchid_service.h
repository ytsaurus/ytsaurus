#pragma once

#include "common.h"
#include "orchid_service_proxy.h"

#include <ytlib/ytree/public.h>
#include <ytlib/rpc/service_detail.h>

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
    typedef TOrchidService TThis;

    NYTree::IYPathServicePtr RootService;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

