#pragma once

#include <yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/ytlib/transaction_client/transaction_ypath.pb.h>

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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
