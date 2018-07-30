#pragma once

#include "public.h"

#include <yt/server/clickhouse/interop/api.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

const NInterop::IPathService* GetPathService();

}   // namespace NClickHouse
}   // namespace NYT
