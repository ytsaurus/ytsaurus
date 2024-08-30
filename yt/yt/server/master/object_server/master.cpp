#include "master.h"
#include "object.h"

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

std::string TMasterObject::GetLowercaseObjectName() const
{
    return "master";
}

std::string TMasterObject::GetCapitalizedObjectName() const
{
    return "Master";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
