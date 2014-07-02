#pragma once

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Transaction";
    }

    // NB: Not logged.
    DEFINE_YPATH_PROXY_METHOD(NProto, Ping);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Commit);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Abort);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
