#pragma once

#include "public.h"

#include <yt/yt/client/transaction_client/public.h>

#include <yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampSegmentMeta
{
    NTransactionClient::TTimestamp MinTimestamp;
    ui32 ExpectedWritesPerRow;
    ui32 ExpectedDeletesPerRow;
};

struct TIntegerSegmentMeta
{
    ui64 MinValue;
};

struct TStringSegmentMeta
{
    ui32 ExpectedLength;
};

struct TDenseVersionedSegmentMeta
{
    ui32 ExpectedValuesPerRow;
};

struct TSchemalessSegmentMeta
{
    ui32 ExpectedBytesPerRow;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TTimestampSegmentMeta* protoTimestampSegmentMeta,
    const TTimestampSegmentMeta& timestampSegmentMeta) ;

void FromProto(
    TTimestampSegmentMeta* timestampSegmentMeta,
    const NProto::TTimestampSegmentMeta& protoTimestampSegmentMeta) ;

void ToProto(
    NProto::TIntegerSegmentMeta* protoIntegerSegmentMeta,
    const TIntegerSegmentMeta& integerSegmentMeta);
void FromProto(
    TIntegerSegmentMeta* integerSegmentMeta,
    const NProto::TIntegerSegmentMeta& protoIntegerSegmentMeta);

void ToProto(
    NProto::TStringSegmentMeta* protoStringSegmentMeta,
    const TStringSegmentMeta& stringSegmentMeta);

void FromProto(
    TStringSegmentMeta* stringSegmentMeta,
    const NProto::TStringSegmentMeta& protoStringSegmentMeta);

void ToProto(
    NProto::TDenseVersionedSegmentMeta* protoDenseVersionedSegmentMeta,
    const TDenseVersionedSegmentMeta& denseVersionedSegmentMeta);

void FromProto(
    TDenseVersionedSegmentMeta* denseVersionedSegmentMeta,
    const NProto::TDenseVersionedSegmentMeta& protoDenseVersionedSegmentMeta);

void ToProto(
    NProto::TSchemalessSegmentMeta* protoSchemalessSegmentMeta,
    const TSchemalessSegmentMeta& schemalessSegmentMeta);

void FromProto(
    TSchemalessSegmentMeta* schemalessSegmentMeta,
    const NProto::TSchemalessSegmentMeta& protoSchemalessSegmentMeta);

////////////////////////////////////////////////////////////////////////////////

struct TSegmentMeta
{
    int Version;
    int Type;
    i64 RowCount;
    int BlockIndex;
    i64 Offset;
    i64 ChunkRowCount;
    i64 Size;

    // Segment meta extensions.
    std::optional<TTimestampSegmentMeta> TimestampSegmentMeta;
    std::optional<TIntegerSegmentMeta> IntegerSegmentMeta;

    std::optional<TStringSegmentMeta> StringSegmentMeta;
    std::optional<TDenseVersionedSegmentMeta> DenseVersionedSegmentMeta;
    std::optional<TSchemalessSegmentMeta> SchemalessSegmentMeta;

    //! This function is used for keeping TSegmentMeta syncronized with NProto::TSegmentMeta.
    //! Returns count of extensions that were added into TSegmentMeta.
    static int GetExtensionCount();
};

struct TColumnMeta
{
    std::vector<TSegmentMeta> Segments;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TSegmentMeta* protoSegmentMeta,
    const TSegmentMeta& segmentMeta);
void FromProto(
    TSegmentMeta* segmentMeta,
    const NProto::TSegmentMeta& protoSegmentMeta);

void ToProto(
    NProto::TColumnMeta* protoColumnMeta,
    const TColumnMeta& columnMeta);
void FromProto(
    TColumnMeta* columnMeta,
    const NProto::TColumnMeta& protoColumnMeta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
