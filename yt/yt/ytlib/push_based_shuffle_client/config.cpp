#include "config.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>

namespace NYT::NPushBasedShuffleClient {

using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

void TShuffleWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("memory_budget", &TThis::MemoryBudget)
        .GreaterThanOrEqual(1_MB)
        .Default(64_MB);
    registrar.Parameter("builders_budget_fraction", &TThis::BuildersBudgetFraction)
        .InRange(0.01, 0.99)
        .Default(0.8);
    registrar.Parameter("codec", &TThis::Codec)
        .Default(ECodec::Lz4);
    registrar.Parameter("max_send_attempts", &TThis::MaxSendAttempts)
        .GreaterThan(0)
        .Default(3);
    registrar.Parameter("writer_config", &TThis::WriterConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
