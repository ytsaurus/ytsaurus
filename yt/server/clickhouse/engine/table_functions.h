#pragma once

#include <yt/server/clickhouse/interop/api.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsExt(NInterop::IStoragePtr storage);

}   // namespace NClickHouse
}   // namespace NYT
