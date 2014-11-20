#pragma once

#include "public.h"

#include "row_base.h"

#include <ytlib/chunk_client/schema.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TableClientLogger;

extern NProfiling::TProfiler TableReaderProfiler;

template <class TPredicate>
int LowerBound(int lowerIndex, int upperIndex, const TPredicate& less)
{
    while (upperIndex - lowerIndex > 0) {
        auto middle = (upperIndex + lowerIndex) / 2;
        if (less(middle)) {
            lowerIndex = middle + 1;
        } else {
            upperIndex = middle;
        }
    }
    return lowerIndex;
}

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns);

TColumnFilter CreateColumnFilter(const NChunkClient::TChannel& protoChannel, TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
