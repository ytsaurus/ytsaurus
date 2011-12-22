#pragma once

#include "common.h"
#include "orchid_service_proxy.h"

#include "../ytree/ytree.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TOrchidService> TPtr;

    //! Creates an instance.
    TOrchidService(
        NYTree::INode* root,
        NRpc::IRpcServer* server,
        IInvoker* invoker);

private:
    typedef TOrchidService TThis;

    NYTree::INode::TPtr Root;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

