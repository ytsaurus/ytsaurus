#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NHydra {

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

    //! Clears the instance.
    virtual void Clear() = 0;

    //! Brings the instance it to the state corresponding to zero version.
    //! It is guaranteed that the instance is cleared (via #Clear) prior to this call.
    virtual void SetZeroState() = 0;

    //! Applies a certain deterministic mutation to the instance.
    virtual void ApplyMutation(TMutationContext* context) = 0;

    //! Returns global context version, typicaly the snapshot version for the component.
    virtual TReign GetCurrentReign() = 0;

    //! Returns action that needs to be done after replaying changelog from a scecific reign.
    virtual EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign) = 0;

    //! Returns the resulting action that needs to be done after replaying changelog.
    virtual EFinalRecoveryAction GetFinalRecoveryAction() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
