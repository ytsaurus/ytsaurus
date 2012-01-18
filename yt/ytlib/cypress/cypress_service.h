#pragma once

#include "common.h"
#include "cypress_service_proxy.h"
#include "cypress_manager.h"

#include <ytlib/rpc/server.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TCypressService> TPtr;

    //! Creates an instance.
    TCypressService(
        IInvoker* invoker,
        TCypressManager* cypressManager);

private:
    typedef TCypressService TThis;

    TCypressManager::TPtr CypressManager;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
