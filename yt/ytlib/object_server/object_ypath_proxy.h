#pragma once

#include "object_ypath.pb.h"

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, GetId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
