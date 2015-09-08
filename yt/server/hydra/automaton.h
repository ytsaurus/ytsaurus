#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/concurrency/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! An abstract automaton replicated via Hydra.
struct IAutomaton
    : public virtual TRefCounted
{
    //! Performs the synchronous phase of snapshot serialization and initiates
    //! the asynchronous phase. Returns an async flag indicating completion of the latter.
    virtual TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer) = 0;

    //! Synchronously loads a snapshot.
    //! It is guaranteed that the instance is cleared (via #Clear) prior to this call.
    virtual void LoadSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader) = 0;

    //! Clears the instance bringing it to the initial state.
    virtual void Clear() = 0;

    //! Applies a certain deterministic mutation to the instance.
    virtual void ApplyMutation(TMutationContext* context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
