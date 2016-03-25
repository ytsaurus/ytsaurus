#include "private.h"
#include "name_table.h"

#include <yt/ytlib/chunk_client/schema.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

namespace NYT {
namespace NTableClient {

using NChunkClient::TChannel;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TableClientLogger("TableClient");
const NProfiling::TProfiler TableClientProfiler("/table_client");

///////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns)
{
    if (chunkKeyColumns.size() < keyColumns.size()) {
        THROW_ERROR_EXCEPTION("Chunk has less key columns than requested: actual %v, expected %v",
            chunkKeyColumns,
            keyColumns);
    }

    for (int i = 0; i < keyColumns.size(); ++i) {
        if (chunkKeyColumns[i] != keyColumns[i]) {
            THROW_ERROR_EXCEPTION("Incompatible key columns: actual %v, expected %v",
                chunkKeyColumns,
                keyColumns);
        }
    }
}

TColumnFilter CreateColumnFilter(const NChunkClient::TChannel& channel, TNameTablePtr nameTable)
{
    TColumnFilter columnFilter;
    if (channel.IsUniversal()) {
        return columnFilter;
    }

    // Ranges are not supported since 0.17
    YCHECK(channel.GetRanges().empty());


    columnFilter.All = false;
    for (auto column : channel.GetColumns()) {
        auto id = nameTable->GetIdOrRegisterName(column);
        columnFilter.Indexes.push_back(id);
    }

    return columnFilter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
