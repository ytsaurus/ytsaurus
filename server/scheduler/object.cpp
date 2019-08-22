#include "object.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TObject::TObject(
    TObjectId id,
    TYsonString labels)
    : Id_(std::move(id))
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

} // namespace NYP::NServer::NScheduler
