#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>

#include <memory>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGeoDictionariesLoader> CreateGeoDictionariesLoader(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string& geodataPath);

} // namespace NYT::NClickHouseServer::NEngine
