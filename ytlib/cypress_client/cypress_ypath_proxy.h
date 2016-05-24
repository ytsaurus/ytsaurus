#pragma once

#include "public.h"

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>

#include <yt/ytlib/object_client/object_ypath_proxy.h>

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
