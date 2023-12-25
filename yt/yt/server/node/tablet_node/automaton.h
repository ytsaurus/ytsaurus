#pragma once

#include "public.h"
#include "serialize.h"
#include "mutation_forwarder.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/logger_owner.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra automaton managing a number of tablets.
class TTabletAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    TTabletAutomaton(
        TCellId cellId,
        IInvokerPtr asyncSnapshotInvoker,
        NLeaseServer::ILeaseManagerPtr leaseManager);

private:
    const NLeaseServer::ILeaseManagerPtr LeaseManager_;

    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        NHydra::ICheckpointableInputStream* input) override;

    NHydra::TReign GetCurrentReign() override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TTabletAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TTabletAutomatonPart
    : public NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    TTabletAutomatonPart(
        TCellId cellId,
        NHydra::ISimpleHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IMutationForwarderPtr mutationForwarder);

    bool ValidateSnapshotVersion(int version) override;
    int GetCurrentSnapshotVersion() override;

    //! Like |RegisterMethod|, but the mutation will be forwarded to a target servant
    //! if the subject tablet is a source servant in a smooth movement process.
    /*!
     *  NB. TRequest must have tablet_id field.
     */
    template <class TRequest>
    void RegisterForwardedMethod(TCallback<void(TRequest*)> callback);

protected:
    const IMutationForwarderPtr MutationForwarder_;

private:
    template <class TRequest>
    void ForwardedMethodImpl(TCallback<void(TRequest*)> callback, TRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define AUTOMATON_INL_H_
#include "automaton-inl.h"
#undef AUTOMATON_INL_H_
