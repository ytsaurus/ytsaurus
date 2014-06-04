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

NProfiling::TProfiler TableReaderProfiler("/table_reader");

///////////////////////////////////////////////////////////////////////////////

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

} // namespace NVersionedTableClient
} // namespace NYT
