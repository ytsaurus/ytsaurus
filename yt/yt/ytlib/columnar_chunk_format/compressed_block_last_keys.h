#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/ytlib/columnar_chunk_format/read_span.h>

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct IBlockLastKeys
{
    virtual NTableClient::TKeyRef GetBlockLastKey(int rowIndex, std::vector<TUnversionedValue>* buffer) const = 0;

    virtual std::vector<TSpanMatching> BuildReadListForWindow(TRange<TRowRange> keyRanges, const NTableClient::TTableSchema& tableSchema) const = 0;
    virtual std::vector<TSpanMatching> BuildReadListForWindow(TRange<TLegacyKey> keys, const NTableClient::TTableSchema& tableSchema) const = 0;

    virtual i64 GetByteSize() const = 0;

    virtual ~IBlockLastKeys() = default;
};

std::unique_ptr<IBlockLastKeys> CompressBlockLastKeys(
    TRange<TUnversionedRow> blockLastKeys,
    std::vector<ui32> chunkRowCountsUnique,
    NTableClient::TTableSchemaPtr chunkSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
