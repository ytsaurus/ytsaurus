#include "object.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TObject::TObject(
    TObjectId id,
    NYT::NYson::TYsonString labels)
    : Id_(std::move(id))
    , Labels_(std::move(labels))
{ }

const TObjectId& TObject::GetId() const
{
    return Id_;
}

const NYT::NYson::TYsonString& TObject::GetLabels() const
{
    return Labels_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
