#pragma once

#include "common.h"
#include "orchid_service_rpc.h"

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
        NRpc::IServer* server,
        IInvoker* invoker);

private:
    typedef TOrchidService TThis;
    typedef TOrchidServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TServiceException TServiceException;

    NYTree::INode::TPtr Root;

    RPC_SERVICE_METHOD_DECL(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

