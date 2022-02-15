#include "simple_transaction_supervisor.h"

#include <yt/yt/server/lib/hive/transaction_manager.h>

namespace NYT::NHiveServer {

using namespace NHydra;
using namespace NLogging;
using namespace NProto;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void RecoverErrorFromMutationResponse(TMutationResponse response)
{
    const auto& data = response.Data;
    NRpc::NProto::TResponseHeader header;
    NRpc::TryParseResponseHeader(data, &header);
    if (header.has_error()) {
        FromProto<TError>(header.error())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

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

TFuture<void> TSimpleTransactionSupervisor::PrepareTransactionCommit(
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
    return mutation->Commit().Apply(BIND(&RecoverErrorFromMutationResponse));
}

TFuture<void> TSimpleTransactionSupervisor::CommitTransaction(
    TTransactionId transactionId,
    TTimestamp commitTimestamp)
{
    NProto::TReqCommitTransaction request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_commit_timestamp(commitTimestamp);

    auto mutation = CreateMutation(HydraManager_, request);
    mutation->SetCurrentTraceContext();
    return mutation->Commit().Apply(BIND(&RecoverErrorFromMutationResponse));
}

void TSimpleTransactionSupervisor::HydraPrepareTransactionCommit(TReqPrepareTransactionCommit* request)
{
    TransactionManager_->PrepareTransactionCommit(
        FromProto<TGuid>(request->transaction_id()),
        request->persistent(),
        request->prepare_timestamp(),
        InvalidCellTag,
        /*prerequisiteTransactionIds*/ {});
}

void TSimpleTransactionSupervisor::HydraCommitTransaction(TReqCommitTransaction* request)
{
    TransactionManager_->CommitTransaction(
        FromProto<TGuid>(request->transaction_id()),
        request->commit_timestamp(),
        InvalidCellTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
