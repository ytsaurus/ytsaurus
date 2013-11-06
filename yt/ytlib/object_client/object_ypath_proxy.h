#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/object_client/object_ypath.pb.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TObjectYPathProxy
    : public NYTree::TYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Object";
    }

    DEFINE_YPATH_PROXY_METHOD(NProto, GetId);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckPermission);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
