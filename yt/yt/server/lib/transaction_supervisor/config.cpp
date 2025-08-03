#include "config.h"

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

void TTransactionSupervisorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("participant_probation_period", &TThis::ParticipantProbationPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("participant_backoff_time", &TThis::ParticipantBackoffTime)
        .Default(TDuration::Seconds(5));
    registrar.Parameter(
        "enable_wait_until_prepared_transactions_finished",
        &TThis::EnableWaitUntilPreparedTransactionsFinished)
        .Default(false);
    registrar.Parameter(
        "validate_strongly_ordered_transaction_refs",
        &TThis::ValidateStronglyOrderedTransactionRefs)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionLeaseTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_count", &TThis::ThreadCount)
        .Default(1)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
