#pragma once

#include "cypress_service_proxy.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/cell_master/meta_state_service.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TCypressService> TPtr;

    //! Creates an instance.
    TCypressService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TCypressService TThis;
    class TExecuteSession;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
