#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct IColumnReaderBase
    : public TNonCopyable
{
    virtual ~IColumnReaderBase() = default;

    //! Feeds a new block into the reader.
    virtual void SetCurrentBlock(TSharedRef block, int blockIndex) = 0;

    //! Positions the reader at a given #rowIndex.
    //! Reader can only be moved forward.
    //! #rowIndex must remain within the current segment (possibly at the very end of it).
    virtual void SkipToRowIndex(i64 rowIndex) = 0;

    //! If the current row index points to the end of the segment,
    //! the segment is switched to the next one.
    virtual void Rearm() = 0;

    //! First unread row index.
    virtual i64 GetCurrentRowIndex() const = 0;

    //! First row index outside the current block.
    virtual i64 GetBlockUpperRowIndex() const = 0;

    //! Index of the first row  outside the range that is ready to be
    //! read without block or segment switch.
    virtual i64 GetReadyUpperRowIndex() const = 0;

    //! Returns current block index or `-1` if no block was set.
    virtual int GetCurrentBlockIndex() const = 0;

    //! Returns current segment index.
    virtual int GetCurrentSegmentIndex() const = 0;

    //! Returns the index of the next block needed by the reader.
    virtual std::optional<int> GetNextBlockIndex() const = 0;
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

    //! Reads values into proper position inside rows.
    virtual void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    //! Read values into proper position inside rows.
    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) = 0;

    //! Returns the number of IUnversionedColumnarRowBatch::TColumn instances needed to represent
    //! the current segment.
    virtual int GetBatchColumnCount() = 0;

    //! Reads values into #columns (of size #GetBatchColumnCount).
    virtual void ReadColumnarBatch(
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns,
        i64 rowCount) = 0;

    //! Gives an estimate for data weight in [lowerRowIndex, upperRowIndex) row range.
    //! The range must be within the current segment.
    /*!
     *  This is an estimate for at least two reasons:
     *  1) all values are assumed to be non-null
     *  2) for strings, the average (over the segment) length is used
     */
    // TODO(babenko): add density parameter to segment meta to solve 1), at least partially.
    virtual i64 EstimateDataWeight(i64 lowerRowIndex, i64 upperRowIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedColumnReader(
    const NTableClient::TColumnSchema& schema,
    const NProto::TColumnMeta& meta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

////////////////////////////////////////////////////////////////////////////////

//! Reads schemaless parts of unverisoned chunks.
struct ISchemalessColumnReader
    : public virtual IColumnReaderBase
{
    //! Writes schemaless values into rows.
    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) = 0;

    //! For rows in range [CurrentRowIndex, CurrentRowIndex + valueCounts.Size())
    //! writes the number of schemaless values per row.
    virtual void ReadValueCounts(TMutableRange<ui32> valueCounts) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISchemalessColumnReader> CreateSchemalessColumnReader(
    const NProto::TColumnMeta& meta,
    const std::vector<int>& idMapping);

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
        TRange<std::pair<ui32, ui32>> timestampIndexRanges,
        bool produceAllVersions) = 0;


    //! Read values for compaction.
    virtual void ReadAllValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    //! For rows from #CurrentRowIndex to (#CurrentRowIndex + valueCounts.Size())
    //! return number of values per row.
    virtual void ReadValueCounts(TMutableRange<ui32> valueCounts) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const NTableClient::TColumnSchema& columnSchema,
    const NProto::TColumnMeta& meta,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
