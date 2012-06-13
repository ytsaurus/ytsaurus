#pragma once

#include <ytlib/object_server/object_ypath_proxy.h>
#include <ytlib/transaction_server/transaction_ypath.pb.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern NYTree::TYPath RootTransactionPath;

////////////////////////////////////////////////////////////////////////////////

struct TTransactionYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Commit);
    DEFINE_YPATH_PROXY_METHOD(NProto, Abort);
    DEFINE_YPATH_PROXY_METHOD(NProto, RenewLease);

    DEFINE_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_YPATH_PROXY_METHOD(NProto, ReleaseObject);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
