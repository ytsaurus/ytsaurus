#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/ytree/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/hive/transaction_supervisor_service.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionManager
    : public virtual TRefCounted
{
    virtual void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp) = 0;

    //! Once #PrepareTransactionCommit succeeded, #CommitTransaction cannot throw.
    virtual void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp) = 0;

    virtual void AbortTransaction(const TTransactionId& transactionId) = 0;

    virtual void PingTransaction(
        const TTransactionId& transactionId,
        const NProto::TReqPingTransaction& request) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
