#pragma once

#include "public.h"

#include <yt/yt/client/complex_types/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkColumnMapping final
{
public:
    TChunkColumnMapping(const TTableSchemaPtr& tableSchema, const TTableSchemaPtr& chunkSchema);

    std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
        const TColumnFilter& columnFilter) const;

    std::vector<int> BuildSchemalessHorizontalSchemaIdMapping(
        const TColumnFilter& columnFilter) const;

private:
    const int TableKeyColumnCount_;
    const int ChunkKeyColumnCount_;
    const int ChunkColumnCount_;

    // For each value (aka non-key) column of the table, store its index
    // in the chunk schema. Note that this vector is indexed by relative
    // index of the value column, so 0 for the first non-key column, 1 for
    // the second, etc.
    // TODO(s-berdnikov): Consider changing -1 as a sentinel to std::nullopt.
    const std::vector<int> TableValueIndexToChunkIndex_;
};

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
