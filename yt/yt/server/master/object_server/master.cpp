#include "master.h"
#include "object.h"

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TMasterObject::TMasterObject(TObjectId id)
    : TNonversionedObjectBase(id)
{ }

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
