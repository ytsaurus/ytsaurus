#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/object_client/master_ypath.pb.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TMasterYPathProxy
    : public NYTree::TYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Master";
    }

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CreateObjects);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, UnstageObject);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
