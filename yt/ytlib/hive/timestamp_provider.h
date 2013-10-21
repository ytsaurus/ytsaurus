#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct ITimestampProvider
    : public virtual TRefCounted
{
    virtual TFuture<TErrorOr<TTimestamp>> GetTimestamp() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

