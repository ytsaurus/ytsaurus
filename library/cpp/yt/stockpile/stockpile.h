#pragma once

#include <util/system/types.h>
#include <util/generic/size_literals.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TStockpileOptions
{
    i64 BufferSize = 4_GB;

    int ThreadCount = 4;

    TDuration StockpilePeriod = TDuration::MilliSeconds(10);
};

void StockpileMemory(TStockpileOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
