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

    registrar.Parameter("fetch_chunk_meta_from_sequoia", &TThis::FetchChunkMetaFromSequoia)
        .Default(false);

    registrar.Parameter("sequoia_queue", &TThis::SequoiaQueue)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
