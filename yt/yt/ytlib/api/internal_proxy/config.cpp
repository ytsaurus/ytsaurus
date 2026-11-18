#include "config.h"

namespace NYT::NApi::NInternalProxy {

////////////////////////////////////////////////////////////////////////////////

void TQueueExportRunnerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_table_count_per_call", &TThis::MaxTableCountPerCall)
        .Default(10)
        .GreaterThan(0);
    registrar.Parameter("max_hunk_chunk_count_per_fetch", &TThis::MaxHunkChunkCountPerFetch)
        .Default(1000)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NInternalProxy
