#include "config.h"

#include <yt/yt/ytlib/chaos_client/config.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

void TChaosCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_cards_watcher", &TThis::ReplicationCardsWatcher)
        .DefaultNew();

    registrar.Parameter("unwatched_cards_expiration_delay", &TThis::UnwatchedCardExpirationDelay)
        .Default(TDuration::Minutes(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
