#pragma once

#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/core/ytree/ypath_proxy.h>

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

    DEFINE_YPATH_PROXY_METHOD(NProto, GetBasicAttributes);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckPermission);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
