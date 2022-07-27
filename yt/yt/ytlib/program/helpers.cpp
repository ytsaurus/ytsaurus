#include "helpers.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/program/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureNativeSingletons(const TNativeSingletonsConfigPtr& config)
{
    ConfigureSingletons(static_cast<TSingletonsConfigPtr>(config));

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
}

void ReconfigureNativeSingletons(
    const TNativeSingletonsConfigPtr& config,
    const TNativeSingletonsDynamicConfigPtr& dynamicConfig)
{
    ReconfigureSingletons(
        static_cast<TSingletonsConfigPtr>(config),
        static_cast<TSingletonsDynamicConfigPtr>(dynamicConfig));

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher->ApplyDynamic(dynamicConfig->ChunkClientDispatcher));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
