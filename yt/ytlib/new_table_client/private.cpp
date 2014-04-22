#include "stdafx.h"
#include "private.h"

#include "name_table.h"

#include <ytlib/chunk_client/schema.h>

#include <core/misc/error.h>

#include <core/misc/protobuf_helpers.h>

#include <core/misc/string.h>

namespace NYT {
namespace NVersionedTableClient {

using NChunkClient::TChannel;

////////////////////////////////////////////////////////////////////////////////

const NLog::TLogger TableClientLogger("TableReader");

///////////////////////////////////////////////////////////////////////////////

int LowerBound(int lowerIndex, int upperIndex, std::function<bool(int)> less)
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

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns)
{
    if (chunkKeyColumns.size() < keyColumns.size()) {
        THROW_ERROR_EXCEPTION(
            "Chunk has less key columns than requested (Actual: [%s], Expected: [%s])",
            ~JoinToString(chunkKeyColumns),
            ~JoinToString(keyColumns));
    }

    for (int i = 0; i < keyColumns.size(); ++i) {
        if (chunkKeyColumns[i] != keyColumns[i]) {
            THROW_ERROR_EXCEPTION(
                "Incompatible key columns (Actual: [%s], Expected: [%s])",
                ~JoinToString(chunkKeyColumns),
                ~JoinToString(keyColumns));
        }
    }
}

TColumnFilter CreateColumnFilter(const NChunkClient::NProto::TChannel& protoChannel, TNameTablePtr nameTable)
{
    TChannel channel = NYT::FromProto<TChannel>(protoChannel);

    // Ranges are not supported since 0.17
    YCHECK(channel.GetRanges().empty());

    TColumnFilter columnFilter;
    if (channel.IsUniversal()) {
        columnFilter.All = true;
        return columnFilter;
    }

    for (auto column : channel.GetColumns()) {
        auto id = nameTable->GetIdOrRegisterName(column);
        columnFilter.Indexes.push_back(id);
    }
    
    return columnFilter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
