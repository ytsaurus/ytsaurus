#pragma once

#include "id.h"
#include "cypress_ypath.pb.h"

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_YPATH_PROXY_METHOD(NProto, Create);
};

////////////////////////////////////////////////////////////////////////////////

extern const NYTree::TYPath ObjectIdMarker;

NYTree::TYPath FromObjectId(const NObjectServer::TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
