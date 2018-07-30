#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Interpreters/IRuntimeComponentsFactory.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IRuntimeComponentsFactory> CreateRuntimeComponentsFactory(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    std::string homePath);

} // namespace NClickHouse
} // namespace NYT
