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
    Save(context, LastSeenTime_);
    Save(context, SequoiaReign_);
}

void TCypressProxyObject::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;

    Load(context, Address_);
    Load(context, LastSeenTime_);
    Load(context, SequoiaReign_);
}

std::string TCypressProxyObject::GetLowercaseObjectName() const
{
    return "cypress proxy " + Address_;
}

std::string TCypressProxyObject::GetCapitalizedObjectName() const
{
    return "Cypress Proxy " + Address_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
