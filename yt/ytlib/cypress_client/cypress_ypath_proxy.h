#pragma once

#include "public.h"

#include <ytlib/object_server/object_ypath_proxy.h>
#include <ytlib/cypress_client/cypress_ypath.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates the YPath pointing to an object with a given id.
NYTree::TYPath FromObjectId(const TObjectId& id);

//! Prepends a given YPath with transaction id marker.
NYTree::TYPath WithTransaction(const NYTree::TYPath& path, const TTransactionId& id);

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_YPATH_PROXY_METHOD(NProto, Copy);
    DEFINE_YPATH_PROXY_METHOD(NProto, Move);
};

////////////////////////////////////////////////////////////////////////////////
} // namespace NCypressClient
} // namespace NYT
