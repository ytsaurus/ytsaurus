#include "configure_singletons.h"
#include "config.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/tracing/trace_manager.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/concurrency/fiber.h>

#include <yt/core/misc/lfalloc_helpers.h>

#include <yt/core/net/local_address.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    NLFAlloc::SetEnableDefrag(false);

    for (const auto& pair : config->FiberStackPoolSizes) {
        NConcurrency::SetFiberStackPoolSize(ParseEnum<NConcurrency::EExecutionStackKind>(pair.first), pair.second);
    }

    NLogging::TLogManager::Get()->Configure(config->Logging);

    NNet::TAddressResolver::Get()->Configure(config->AddressResolver);
    if (!NNet::TAddressResolver::Get()->IsLocalHostNameOK()) {
        THROW_ERROR_EXCEPTION("Could not determine local host FQDN");
    }

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher);

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);

    NTracing::TTraceManager::Get()->Configure(config->Tracing);

    NProfiling::TProfileManager::Get()->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
