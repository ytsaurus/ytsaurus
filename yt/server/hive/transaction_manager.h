#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/ytree/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionManager
    : public virtual TRefCounted
{
    virtual NTransactionClient::TTransactionId StartTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        NTransactionClient::TTimestamp startTimestamp,
        const NTransactionClient::TTransactionId& parentTransactionId,
        NYTree::IAttributeDictionary* attributes,
        const TNullable<TDuration>& timeout) = 0;

    virtual void CommitTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        NTransactionClient::TTimestamp commitTimestamp) = 0;

    virtual void PrepareTransactionCommit(
        const NTransactionClient::TTransactionId& transactionId,
        NTransactionClient::TTimestamp prepareTimestamp) = 0;

    virtual void AbortTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;

    virtual void PingTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
