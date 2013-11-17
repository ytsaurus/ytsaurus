#pragma once

#include "public.h"

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! |#|-prefix.
extern TStringBuf ObjectIdPathPrefix;

//! Creates the YPath pointing to an object with a given id.
NYPath::TYPath FromObjectId(const TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Copy);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
