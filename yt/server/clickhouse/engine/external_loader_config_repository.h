#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateExternalLoaderConfigRepository(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    const std::string& path);

} // namespace NClickHouse
} // namespace NYT
