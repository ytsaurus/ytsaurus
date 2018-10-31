#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Interpreters/IRuntimeComponentsFactory.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    NNative::IStoragePtr storage,
    std::string cliqueId,
    NNative::IAuthorizationTokenPtr authToken,
    std::string homePath,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
