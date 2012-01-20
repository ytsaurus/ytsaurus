#pragma once

#include "id.h"
#include "cypress_ypath.pb.h"

#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
};

////////////////////////////////////////////////////////////////////////////////

extern const NYTree::TYPath ObjectIdMarker;

NYTree::TYPath FromObjectId(const NObjectServer::TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
