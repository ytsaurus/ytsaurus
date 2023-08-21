#pragma once

#include "public.h"

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
    int TableKeyColumnCount_;
    int ChunkKeyColumnCount_;
    int ChunkColumnCount_;
    // Mapping from table schema value index to chunk schema value id.
    // Schema value index is value id minus keyColumnCount.
    std::vector<int> ValueIdMapping_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
