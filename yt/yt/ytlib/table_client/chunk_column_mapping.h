#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkColumnMapping final
{
public:
    TChunkColumnMapping(
        const TTableSchemaPtr& tableSchema,
        const TTableSchemaPtr& chunkSchema);

    std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
        const TColumnFilter& columnFilter) const;

    std::vector<int> BuildSchemalessHorizontalSchemaIdMapping(
        const TColumnFilter& columnFilter) const;

private:
    const int TableKeyColumnCount_;
    const int ChunkKeyColumnCount_;
    const int ChunkColumnCount_;
    // Mapping from table schema value index to chunk schema value id.
    // Schema value index is value id minus key column count.
    std::vector<int> ValueIdMapping_;
};

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
