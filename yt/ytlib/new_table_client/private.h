#pragma once

#include "public.h"

#include <ytlib/chunk_client/schema.pb.h>

#include <core/logging/log.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TableClientLogger;

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

TColumnFilter CreateColumnFilter(const NChunkClient::NProto::TChannel& protoChannel, TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
