#include "config.h"

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableUpdateQueueConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("flush_batch_size", &TThis::FlushBatchSize)
        .Default(1000);

    registrar.Parameter("pause_flush", &TThis::PauseFlush)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

const TDynamicTableUpdateQueueConfigPtr& TDynamicGroundUpdateQueueManagerConfig::GetQueueConfig(NSequoiaClient::EGroundUpdateQueue queue) const
{
    static const auto defaultConfig = New<TDynamicTableUpdateQueueConfig>();
    auto it = Queues.find(queue);
    return it == Queues.end() ? defaultConfig : it->second;
}

void TDynamicGroundUpdateQueueManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("queues", &TThis::Queues)
        .Default();

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCypressProxyTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_proxy_orchid_timeout", &TThis::CypressProxyOrchidTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSequoiaManagerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sequoia_transaction_start_failure_probability", &TThis::SequoiaTransactionStartFailureProbability)
        .GreaterThanOrEqual(0.0)
        .LessThanOrEqual(1.0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSequoiaManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("enable_cypress_transactions_in_sequoia", &TThis::EnableCypressTransactionsInSequoia)
        .Default(false);

    registrar.Parameter("enable_ground_update_queues", &TThis::EnableGroundUpdateQueues)
        .Default(false);

    registrar.Parameter("enable_async_sequoia_transaction_start", &TThis::EnableAsyncSequoiaTransactionStart)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew()
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
