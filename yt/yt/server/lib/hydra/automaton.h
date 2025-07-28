#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotSaveContext
{
    //! Writer where snapshot content must be written to.
    NConcurrency::IAsyncOutputStreamPtr Writer;

    //! Logger to use during snapshot construction.
    NLogging::TLogger Logger;
};

struct TSnapshotLoadContext
{
    //! Reader used for reading snapshot content.
    NConcurrency::IAsyncZeroCopyInputStreamPtr Reader;
};

//! An abstract automaton replicated via Hydra.
struct IAutomaton
    : public virtual TRefCounted
{
    //! Performs the synchronous phase of snapshot serialization and initiates
    //! the asynchronous phase. Returns an async flag indicating completion of the latter.
    virtual TFuture<void> SaveSnapshot(const TSnapshotSaveContext& context) = 0;

    //! Synchronously loads a snapshot.
    //! It is guaranteed that the instance is cleared (via #Clear) prior to this call.
    virtual void LoadSnapshot(const TSnapshotLoadContext& context) = 0;

    //! Synchronously prepares the automaton state for further mutation processing
    //! in a deterministic way.
    //! During automaton preparation hydra context is set while mutation context is not.
    virtual void PrepareState() = 0;

    //! Clears the instance.
    virtual void Clear() = 0;

    //! Brings the instance it to the state corresponding to zero version.
    //! It is guaranteed that the instance is cleared (via #Clear) prior to this call.
    virtual void SetZeroState() = 0;

    //! Applies a certain deterministic mutation to the instance.
    virtual void ApplyMutation(TMutationContext* context) = 0;

    //! Returns global context version, typically the snapshot version for the component.
    virtual TReign GetCurrentReign() = 0;

    //! Return the custom tracto dimension of the global context version, typically the snapshot version for the component.
    virtual TReign GetCurrentTractoReign() = 0;

    //! Returns action that needs to be done after replaying changelog from a scecific reign.
    virtual EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign) = 0;

    //! Returns action that needs to be done after replaying changelog from a scecific tracto reign.
    virtual EFinalRecoveryAction GetActionToRecoverFromTractoReign(TReign reign) = 0;

    //! Returns the resulting action that needs to be done after replaying changelog.
    virtual EFinalRecoveryAction GetFinalRecoveryAction() = 0;

    //! Checks that persistent state invariants are held. This method is called during
    //! snapshot validation and after some mutations in tests.
    virtual void CheckInvariants() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
