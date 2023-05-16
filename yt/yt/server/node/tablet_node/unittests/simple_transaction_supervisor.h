#pragma once

#include <yt/yt/server/node/tablet_node/unittests/proto/simple_transaction_supervisor.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

//! This wrapper is merely an automaton part dedicated for calling ITransactionManager::PrepareTransaction
//! and ITransactionManager::CommitTransaction with given arguments within mutation.
//! It does not know anything about distributed 1pc/2pc.
class TSimpleTransactionSupervisor
    : public NHydra::TCompositeAutomatonPart
{
public:
    TSimpleTransactionSupervisor(
        ITransactionManagerPtr transactionManager,
        NHydra::ISimpleHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);

    TFuture<void> PrepareTransactionCommit(
        TTransactionId transactionId,
        bool persistent,
        TTimestamp prepareTimestamp);

    TFuture<void> CommitTransaction(
        TTransactionId transactionId,
        TTimestamp commitTimestamp);

    TFuture<void> AbortTransaction(
        TTransactionId transactionId,
        bool force);

private:
    const ITransactionManagerPtr TransactionManager_;

    void HydraPrepareTransactionCommit(NProto::TReqPrepareTransactionCommit* request);
    void HydraCommitTransaction(NProto::TReqCommitTransaction* request);
    void HydraAbortTransaction(NProto::TReqAbortTransaction* request);
};

DEFINE_REFCOUNTED_TYPE(TSimpleTransactionSupervisor)
DECLARE_REFCOUNTED_CLASS(TSimpleTransactionSupervisor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
