#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/object_client/master_ypath.pb.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TMasterYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, CreateObjects);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
