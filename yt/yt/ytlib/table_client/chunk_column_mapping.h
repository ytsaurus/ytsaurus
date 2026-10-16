#pragma once

#include "public.h"

#include <yt/yt/client/complex_types/positional_yson_translation.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;

    // Yson representation of complexly-typed data might need to be translated.
    std::optional<NComplexTypes::TPositionalYsonTranslator> Translator;
};

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

    struct TChunkColumnInfo
    {
        int Index;
        std::optional<NComplexTypes::TPositionalYsonTranslator> Translator;
    };

    // For each value (aka non-key) column of the table, store its index
    // in the chunk schema. Note that this vector is indexed by relative
    // index of the value column, so 0 for the first non-key column, 1 for
    // the second, etc.
    const std::vector<std::optional<TChunkColumnInfo>> TableValueIndexToChunkColumnInfo_;
};

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
