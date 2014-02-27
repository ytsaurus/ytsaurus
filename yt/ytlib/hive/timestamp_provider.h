#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: any
 */
struct ITimestampProvider
    : public virtual TRefCounted
{
    //! Generates a new timestamp that is guaranteed to be larger
    //! than all timestamps previously obtained via this instance.
    virtual TFuture<TErrorOr<TTimestamp>> GenerateNewTimestamp() = 0;

    //! Returns the latest timestamp returned from #GenerateNewTimestamp.
    virtual TTimestamp GetLatestTimestamp() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimestampProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

