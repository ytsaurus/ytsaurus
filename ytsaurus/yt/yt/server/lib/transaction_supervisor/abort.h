#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

class TAbort
    : public TRefTracked<TAbort>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

public:
    TAbort(
        TTransactionId transactionId,
        NRpc::TMutationId mutationId);

    TFuture<TSharedRefArray> GetAsyncResponseMessage();
    void SetResponseMessage(TSharedRefArray message);

private:
    TPromise<TSharedRefArray> ResponseMessagePromise_ = NewPromise<TSharedRefArray>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
