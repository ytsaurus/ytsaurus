#pragma once

#include <yt/yt/client/table_chunk_format/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <library/cpp/yt/memory/shared_range.h>

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

using TSegmentMeta = NTableChunkFormat::NProto::TSegmentMeta;
using TTimestampSegmentMeta = NTableChunkFormat::NProto::TTimestampSegmentMeta;
using TIntegerSegmentMeta = NTableChunkFormat::NProto::TIntegerSegmentMeta;
using TStringSegmentMeta = NTableChunkFormat::NProto::TStringSegmentMeta;
using TDenseVersionedSegmentMeta = NTableChunkFormat::NProto::TDenseVersionedSegmentMeta;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TCachedVersionedChunkMetaPtr;
using NTableClient::TRefCountedDataBlockMetaPtr;
using NTableClient::TColumnIdMapping;

using NTableClient::EValueType;

using NTableClient::TLegacyKey;
using NTableClient::TRowRange;
using NTableClient::TUnversionedValue;
using NTableClient::TVersionedValue;
using NTableClient::TMutableVersionedRow;
using NTableClient::TUnversionedRow;
using NTableClient::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

struct TTmpBuffers;

struct TPreparedChunkMeta;

struct TReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

struct IBlockManager;
struct TSpanMatching;
class TGroupBlockHolder;

using TBlockManagerFactory = std::function<std::unique_ptr<IBlockManager>(
    std::vector<TGroupBlockHolder> blockHolders,
    TRange<TSpanMatching> windowsList)>;

struct TKeysWithHints
{
    std::vector<std::pair<ui32, ui32>> RowIndexesToKeysIndexes;
    TSharedRange<NTableClient::TLegacyKey> Keys;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat

