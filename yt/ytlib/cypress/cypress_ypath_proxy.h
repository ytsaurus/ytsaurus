#pragma once

#include "id.h"
#include "cypress_ypath.pb.h"

#include "../ytree/ypath_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_YPATH_PROXY_METHOD(NProto, Create);
};

extern const char NodeIdMarker;

NYTree::TYPath YPathFromNodeId(const TNodeId& nodeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
