#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IExternalLoaderConfigRepository> CreateDummyConfigRepository();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
