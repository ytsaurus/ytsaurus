#pragma once

#include "common.h"
#include "cypress_service_proxy.h"
#include "cypress_manager.h"

#include <ytlib/meta_state/meta_state_service.h>
#include <ytlib/rpc/server.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TCypressService> TPtr;

    //! Creates an instance.
    TCypressService(
        NMetaState::IMetaStateManager* metaStateManager,
        TCypressManager* cypressManager);

private:
    typedef TCypressService TThis;
    class TExecuteSession;

    TCypressManager::TPtr CypressManager;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
