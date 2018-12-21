#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderConfigRepository(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
