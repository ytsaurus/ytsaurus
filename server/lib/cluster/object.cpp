#include "object.h"

#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NCluster {

using namespace NYT::NYTree;

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

IMapNodePtr TObject::ParseLabels() const
{
    return ConvertTo<IMapNodePtr>(Labels_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
