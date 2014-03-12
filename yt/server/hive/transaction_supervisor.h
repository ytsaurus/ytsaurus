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
        NRpc::IServerPtr rpcServer,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager,
        NTransactionClient::ITimestampProviderPtr timestampProvider);

    ~TTransactionSupervisor();

    void Start();
    void Stop();

    NHydra::TMutationPtr CreateAbortTransactionMutation(const NProto::TReqAbortTransaction& request);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
