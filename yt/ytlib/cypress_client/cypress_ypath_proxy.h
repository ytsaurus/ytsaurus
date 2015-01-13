#pragma once

#include "public.h"

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Cypress";
    }

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Copy);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
