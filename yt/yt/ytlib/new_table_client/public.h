#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_chunk_format/public.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

using TSegmentMeta = NTableChunkFormat::NProto::TSegmentMeta;
using TTimestampSegmentMeta = NTableChunkFormat::NProto::TTimestampSegmentMeta;
using TIntegerSegmentMeta = NTableChunkFormat::NProto::TIntegerSegmentMeta;
using TStringSegmentMeta = NTableChunkFormat::NProto::TStringSegmentMeta;
using TDenseVersionedSegmentMeta = NTableChunkFormat::NProto::TDenseVersionedSegmentMeta;

} // namespace NProto

using NChunkClient::TBlock;
using NChunkClient::TBlockFetcher;
using NChunkClient::TBlockFetcherPtr;

using NTableClient::TCachedVersionedChunkMetaPtr;
using NTableClient::TRefCountedDataBlockMetaPtr;
using NTableClient::TColumnIdMapping;

using NTableClient::EValueType;

// For read_span_refiner.h
using NTableClient::TLegacyKey;
using NTableClient::TRowRange;

// For rowset_builder.h
using NTableClient::TRowBuffer;
using NTableClient::TMutableVersionedRow;

// For segment_readers.h
using NTableClient::EValueType;
using NTableClient::TUnversionedValue;
using NTableClient::TVersionedValue;
using NTableClient::TUnversionedRow;
using NTableClient::TTimestamp;

struct TTmpBuffers;

struct TMetaBase;

struct TColumnGroupInfo;

struct TPreparedChunkMeta;

struct TReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

struct IBlockManager;
struct TSpanMatching;
class TGroupBlockHolder;

using TBlockManagerFactory = std::function<std::unique_ptr<IBlockManager>(
    std::vector<TGroupBlockHolder> blockHolders,
    TRange<TSpanMatching> windowsList)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

