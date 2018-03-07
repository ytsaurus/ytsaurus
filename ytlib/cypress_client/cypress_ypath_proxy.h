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
    DEFINE_YPATH_PROXY(Cypress);

    // User-facing.
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Copy);

    // Used internally when implementing List and Get for multicell virtual maps.
    DEFINE_YPATH_PROXY_METHOD(NProto, Enumerate);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
