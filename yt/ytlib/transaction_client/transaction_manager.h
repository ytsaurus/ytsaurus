#pragma once

#include "common.h"
#include "transaction.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TNonCopyable
{
public:
    TTransactionManager(NRpc::IChannel::TPtr channel);
    ITransaction::TPtr StartTransaction();

private:
    class TTransaction;

    typedef yhash_map<TTransactionId, TTransaction> TTransactionMap;
    TTransactionMap TransactionMap;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
