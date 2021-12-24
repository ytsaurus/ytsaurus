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

    std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
        const TColumnFilter& columnFilter) const;

private:
    int SchemaKeyColumnCount_;
    int ChunkKeyColumnCount_;
    int ChunkColumnCount_;
    std::vector<int> Mapping_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
