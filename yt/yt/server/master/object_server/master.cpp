#include "master.h"
#include "object.h"

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TString TMasterObject::GetLowercaseObjectName() const
{
    return "master";
}

TString TMasterObject::GetCapitalizedObjectName() const
{
    return "Master";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
