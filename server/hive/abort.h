#pragma once

#include "public.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TAbort
    : public TRefTracked<TAbort>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

public:
    TAbort(
        const TTransactionId& transactionId,
        const NRpc::TMutationId& mutationId);

    TFuture<TSharedRefArray> GetAsyncResponseMessage();
    void SetResponseMessage(TSharedRefArray message);

private:
    TPromise<TSharedRefArray> ResponseMessagePromise_ = NewPromise<TSharedRefArray>();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
