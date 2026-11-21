#include "cypress_proxy_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

std::string TCypressProxyObject::GetLowercaseObjectName() const
{
    return Format("cypress proxy %Qv", Address_);
}

std::string TCypressProxyObject::GetCapitalizedObjectName() const
{
    return Format("Cypress Proxy %Qv", Address_);
}

void TCypressProxyObject::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Address_);
    PHOENIX_REGISTER_FIELD(2, LastPersistentHeartbeatTime_);
    PHOENIX_REGISTER_FIELD(3, SequoiaReign_);
    PHOENIX_REGISTER_FIELD(4, Version_);
}

PHOENIX_DEFINE_TYPE(TCypressProxyObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
