#pragma once

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Commit);
    DEFINE_YPATH_PROXY_METHOD(NProto, Abort);
    DEFINE_YPATH_PROXY_METHOD(NProto, RenewLease);
    DEFINE_YPATH_PROXY_METHOD(NProto, UnstageObject);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
