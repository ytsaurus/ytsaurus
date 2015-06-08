#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/concurrency/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IAutomaton
    : public virtual TRefCounted
{
    virtual TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer) = 0;
    virtual void LoadSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader) = 0;

    virtual void Clear() = 0;

    virtual void ApplyMutation(TMutationContext* context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
