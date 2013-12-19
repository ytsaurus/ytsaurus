#pragma once

#include "public.h"

#include <core/actions/invoker.h>

#include <core/rpc/public.h>

#include <ytlib/hive/transaction_supervisor_service.pb.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor
    : public TRefCounted
{
public:
    TTransactionSupervisor(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        NRpc::IRpcServerPtr rpcServer,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager,
        NHive::ITimestampProviderPtr timestampProvider);

    ~TTransactionSupervisor();

    void Start();
    void Stop();

    NHydra::TMutationPtr CreateCommitTransactionMutation(const NProto::TReqCommitTransaction& request);
    NHydra::TMutationPtr CreateAbortTransactionMutation(const NProto::TReqAbortTransaction& request);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
