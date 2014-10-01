#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IResponseKeeper
    : public virtual TRefCounted
{
    virtual TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id) = 0;
    virtual void EndRequest(const TMutationId& id, TSharedRefArray response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
