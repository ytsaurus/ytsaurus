#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/client/table_client/versioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct IColumnReaderBase
    : public TNonCopyable
{
    virtual ~IColumnReaderBase() = default;

    virtual void ResetBlock(TSharedRef block, int blockIndex) = 0;

    virtual void SkipToRowIndex(i64 rowIndex) = 0;

    //! First unread row index.
    virtual i64 GetCurrentRowIndex() const = 0;

    //! First row index outside the current block.
    virtual i64 GetBlockUpperRowIndex() const = 0;

    //! First row index outside the range that is ready to be 
    //! read without block or segment change.
    virtual i64 GetReadyUpperRowIndex() const = 0;

    virtual int GetCurrentBlockIndex() const = 0;

    virtual TNullable<int> GetNextBlockIndex() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IUnversionedColumnReader
    : public virtual IColumnReaderBase
{
    /*!
     *  Caller must guarantee, that range of values between #lowerRowIndex
     *  and #upperRowIndex is sorted and doesn't exceed current block. 
     *  Returns range of row indexes with values equal to #value.
     */
    virtual std::pair<i64, i64> GetEqualRange(
        const NTableClient::TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) = 0;

    /*!
     *  Read values into proper position inside rows.
     */
    virtual void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    /*!
     *  Read values into proper position inside rows.
     */
    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedColumnReader(
    const NTableClient::TColumnSchema& schema,
    const NProto::TColumnMeta& meta,
    int columnIndex,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

//! Reads schemaless parts of unverisoned chunks.
struct ISchemalessColumnReader
    : public virtual IColumnReaderBase
{
    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) = 0;

    //! For rows from #CurrentRowIndex to (#CurrentRowIndex + valueCounts.Size())
    //! return number of schemaless values per row.
    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISchemalessColumnReader> CreateSchemalessColumnReader(
    const NProto::TColumnMeta& meta,
    const std::vector<NTableClient::TColumnIdMapping>& idMapping);

////////////////////////////////////////////////////////////////////////////////

struct IVersionedColumnReader
    : public virtual IColumnReaderBase
{
    //! Read values for scan and lookup requests.
    /*!
     *  Read values into proper position  inside row.
     *  Timestamp index should be replaced with a proper timestamp separately.
     *  Caller must not request more rows than reader is ready to emit.
     */
    virtual void ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows,
        TRange<std::pair<ui32, ui32>> timestampIndexRanges) = 0;


    //! Read values for compaction.
    virtual void ReadAllValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    //! For rows from #CurrentRowIndex to (#CurrentRowIndex + valueCounts.Size())
    //! return number of values per row.
    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const NTableClient::TColumnSchema& schema,
    const NProto::TColumnMeta& meta,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
