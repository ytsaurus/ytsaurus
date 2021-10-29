#include "simple_transaction_supervisor.h"

#include <yt/yt/server/lib/hive/transaction_manager.h>

namespace NYT::NHiveServer {

using namespace NHydra;
using namespace NLogging;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("SimpleTransactionSupervisor");

////////////////////////////////////////////////////////////////////////////////

TSimpleTransactionSupervisor::TSimpleTransactionSupervisor(
    ITransactionManagerPtr transactionManager,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : TCompositeAutomatonPart(
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker))
    , TransactionManager_(std::move(transactionManager))
{
    TCompositeAutomatonPart::RegisterMethod(BIND(&TSimpleTransactionSupervisor::HydraPrepareTransactionCommit, Unretained(this)));
    TCompositeAutomatonPart::RegisterMethod(BIND(&TSimpleTransactionSupervisor::HydraCommitTransaction, Unretained(this)));
}

void TSimpleTransactionSupervisor::PrepareTransactionCommit(
    TTransactionId transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    NProto::TReqPrepareTransactionCommit request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_persistent(persistent);
    request.set_prepare_timestamp(prepareTimestamp);

    auto mutation = CreateMutation(HydraManager_, request);
    mutation->SetCurrentTraceContext();
    mutation->CommitAndLog(Logger);
}

void TSimpleTransactionSupervisor::CommitTransaction(
    TTransactionId transactionId,
    TTimestamp commitTimestamp)
{
    NProto::TReqCommitTransaction request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_commit_timestamp(commitTimestamp);

    auto mutation = CreateMutation(HydraManager_, request);
    mutation->SetCurrentTraceContext();
    mutation->CommitAndLog(Logger);
}

void TSimpleTransactionSupervisor::HydraPrepareTransactionCommit(TReqPrepareTransactionCommit* request)
{
    TransactionManager_->PrepareTransactionCommit(
        FromProto<TGuid>(request->transaction_id()),
        request->persistent(),
        request->prepare_timestamp(),
        /*prerequisiteTransactionIds*/ {});
}

void TSimpleTransactionSupervisor::HydraCommitTransaction(TReqCommitTransaction* request)
{
    TransactionManager_->CommitTransaction(
        FromProto<TGuid>(request->transaction_id()),
        request->commit_timestamp());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
