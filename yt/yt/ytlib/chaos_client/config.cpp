#include "config.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

TChaosCellDirectorySynchronizerConfig::TChaosCellDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("sync_period_splay", SyncPeriodSplay)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
