#include "cypress_proxy_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

void TCypressProxyObject::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;

    Save(context, Address_);
    Save(context, LastPersistentHeartbeatTime_);
    Save(context, SequoiaReign_);
    Save(context, Version_);
}

void TCypressProxyObject::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;

    Load(context, Address_);
    Load(context, LastPersistentHeartbeatTime_);
    Load(context, SequoiaReign_);

    if (context.GetVersion() >= EMasterReign::CypressProxyVersion) {
        Load(context, Version_);
    } else {
        Version_ = "unknown";
    }
}

std::string TCypressProxyObject::GetLowercaseObjectName() const
{
    return Format("cypress proxy %Qv", Address_);
}

std::string TCypressProxyObject::GetCapitalizedObjectName() const
{
    return Format("Cypress Proxy %Qv", Address_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
