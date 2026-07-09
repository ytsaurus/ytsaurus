#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IUniqueSeqNoProvider
    : public TRefCounted
{
    struct TResult
    {
        TSystemTimestamp Timestamp;
        TUniqueSeqNo UniqueSeqNo;
    };

    virtual TFuture<TResult> Generate() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUniqueSeqNoProvider);

////////////////////////////////////////////////////////////////////////////////

struct ISeqNoProvider
    : public TRefCounted
{
    // Both methods may context-switch and must be called from a fiber.

    // Returns the next seqno.
    virtual i64 Generate() = 0;

    // Reserves a contiguous batch of |count| seqnos whose first value is a
    // multiple of |alignment| and returns that first value.
    virtual i64 GenerateAlignedBatch(i64 count, i64 alignment) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISeqNoProvider);

////////////////////////////////////////////////////////////////////////////////

IUniqueSeqNoProviderPtr CreateUniqueSeqNoProvider(
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    NObjectClient::TCellTag clockClusterTag);

//! Like #CreateUniqueSeqNoProvider, but retries clock failures over |client| for up to an hour
//! (at most a minute between attempts) instead of failing the caller immediately.
IUniqueSeqNoProviderPtr CreateRetryingUniqueSeqNoProvider(
    NApi::IClientPtr client,
    NObjectClient::TCellTag clockClusterTag,
    IInvokerPtr invoker,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger);

ISeqNoProviderPtr CreateSeqNoProvider(IUniqueSeqNoProviderPtr uniqueProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
