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
    virtual TTransactionId StartTransaction(
        const TTransactionId& transactionId,
        TTimestamp startTimestamp,
        const TTransactionId& parentTransactionId,
        NYTree::IAttributeDictionary* attributes,
        const TNullable<TDuration>& timeout) = 0;

    virtual void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp) = 0;

    virtual void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        TTimestamp prepareTimestamp) = 0;

    virtual void AbortTransaction(const TTransactionId& transactionId) = 0;

    virtual void PingTransaction(const TTransactionId& transactionId) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
