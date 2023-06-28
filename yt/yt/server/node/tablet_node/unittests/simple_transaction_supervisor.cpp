#include "simple_transaction_supervisor.h"

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

namespace NYT::NTransactionSupervisor {

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
    TCompositeAutomatonPart::RegisterMethod(BIND(&TSimpleTransactionSupervisor::HydraAbortTransaction, Unretained(this)));
}

TFuture<void> TSimpleTransactionSupervisor::PrepareTransactionCommit(
    TTransactionId transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    if (!persistent) {
        return BIND([=, this, this_ = MakeStrong(this)] {
            TTransactionPrepareOptions options{
                .Persistent = persistent,
                .PrepareTimestamp = prepareTimestamp,
                .PrepareTimestampClusterTag = TCellTag(0x42),
            };
            TransactionManager_->PrepareTransactionCommit(transactionId, options);
        })
            .AsyncVia(AutomatonInvoker_)
            .Run();
    }

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

TFuture<void> TSimpleTransactionSupervisor::AbortTransaction(
    TTransactionId transactionId,
    bool force)
{
    NProto::TReqAbortTransaction request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_force(force);

    auto mutation = CreateMutation(HydraManager_, request);
    mutation->SetCurrentTraceContext();
    return mutation->Commit().Apply(BIND(&RecoverErrorFromMutationResponse));
}

void TSimpleTransactionSupervisor::HydraPrepareTransactionCommit(TReqPrepareTransactionCommit* request)
{
    TTransactionPrepareOptions options{
        .Persistent = request->persistent(),
        .PrepareTimestamp = FromProto<TTimestamp>(request->prepare_timestamp()),
    };
    TransactionManager_->PrepareTransactionCommit(
        FromProto<TGuid>(request->transaction_id()),
        options);
}

void TSimpleTransactionSupervisor::HydraCommitTransaction(TReqCommitTransaction* request)
{
    TTransactionCommitOptions options{
        .CommitTimestamp = FromProto<TTimestamp>(request->commit_timestamp())
    };
    TransactionManager_->CommitTransaction(
        FromProto<TGuid>(request->transaction_id()),
        options);
}

void TSimpleTransactionSupervisor::HydraAbortTransaction(TReqAbortTransaction* request)
{
    TTransactionAbortOptions options{
        .Force = request->force(),
    };
    TransactionManager_->AbortTransaction(
        FromProto<TGuid>(request->transaction_id()),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
