#include "object.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TObject::TObject(
    const TObjectId& id,
    TYsonString labels)
    : Id_(id)
    , Labels_(std::move(labels))
{ }

const TObjectId& TObject::GetId() const
{
    return Id_;
}

const TYsonString& TObject::GetLabels() const
{
    return Labels_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

