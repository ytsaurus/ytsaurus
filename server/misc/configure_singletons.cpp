#include "configure_singletons.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/tracing/trace_manager.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/logging/log_manager.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureServerSingletons(const TServerConfigPtr& config)
{
    NLogging::TLogManager::Get()->Configure(config->Logging);

    TAddressResolver::Get()->Configure(config->AddressResolver);
    if (!TAddressResolver::Get()->IsLocalHostNameOK()) {
        THROW_ERROR_EXCEPTION("Could not determine local host FQDN");
    }

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);

    NTracing::TTraceManager::Get()->Configure(config->Tracing);

    NProfiling::TProfileManager::Get()->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
