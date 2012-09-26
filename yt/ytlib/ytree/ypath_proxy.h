#pragma once

#include "ypath_client.h"
#include <ytlib/ytree/ypath.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Get);
    DEFINE_YPATH_PROXY_METHOD(NProto, Set);
    DEFINE_YPATH_PROXY_METHOD(NProto, Remove);
    DEFINE_YPATH_PROXY_METHOD(NProto, List);
    DEFINE_YPATH_PROXY_METHOD(NProto, Exists);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
