#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Manages cluster-wide unique timestamps.
/*!
 *  Thread affinity: any
 */
struct ITimestampProvider
    : public virtual TRefCounted
{
    //! Generates a contiguous range of timestamps (of size #count)
    //! that are guaranteed to be larger than all timestamps previously obtained via this instance.
    //! Returns the first timestamp of that range.
    virtual TFuture<TTimestamp> GenerateTimestamps(int count = 1) = 0;

    //! Returns the latest timestamp returned from #GenerateNewTimestamp.
    virtual TTimestamp GetLatestTimestamp() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimestampProvider)

////////////////////////////////////////////////////////////////////////////////


} // namespace NTransactionClient
} // namespace NYT

