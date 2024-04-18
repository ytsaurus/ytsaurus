#include "config.h"

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicSequoiaQueueConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("flush_batch_size", &TThis::FlushBatchSize)
        .Default(1000);

    registrar.Parameter("pause_flush", &TThis::PauseFlush)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSequoiaManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("sequoia_queue", &TThis::SequoiaQueue)
        .DefaultNew();

    registrar.Parameter("enable_cypress_transactions_in_sequoia", &TThis::EnableCypressTransactionsInSequoia)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
