#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>
#include <yt/core/misc/range.h>

#include <vector>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IUnversionedRowBatch
    : public virtual TRefCounted
{
    //! Returns the number of rows in the batch.
    //! This call is cheap (in contrast to #GetRows).
    virtual int GetRowCount() const = 0;

    //! A helper method that returns |true| iff #GetRowCount is zero.
    bool IsEmpty() const;

    //! Returns |true| if the batch is internally represeted by a set of columns.
    //! In this case #MaterializeColumns provides access to columnar data.
    virtual bool IsColumnar() const = 0;

    //! Returns the rows representing the batch.
    //! If the batch is columnar then the rows are materialized on first
    //! call to #MaterializeRows. This call could be slow.
    //! Invoking #MaterializeColumns after this call is forbidden.
    virtual TRange<TUnversionedRow> MaterializeRows() = 0;

    struct TValueBuffer;
    struct TStringBuffer;
    struct TBitmap;
    struct TDictionaryEncoding;
    struct TRleEncoding;
    struct TColumn;

    struct TValueBuffer
    {
        //! Bits per value: 8, 16, 32 or 64.
        int BitWidth = -1;
        
        //! For integer values, values in #Data must be adjusted by adding #BaseValue.
        ui64 BaseValue = 0;

        //! Indicates if zig-zag encoding is used for values.
        bool ZigZagEncoded = false;

        //! Memory region containing the values.
        TRef Data;
    };

    struct TStringBuffer
    {
        //! Controls offset decoding.
        /*
         *  If non-null then k-th value starts at offset
         *    0                                             for k = 0;
         *    AvgLength * k + ZigZagDecode32(values[k - 1]) for k = 1, ..., n-1,
         *  where |values[i]| is the i-th raw unsigned 32-bit value stored in #TColumn::Values.
         * 
         *  Otherwise #TColumn::Values contains raw unsigned 32-bit offsets.
         */
        std::optional<ui32> AvgLength;

        //! Memory region with string data. Offsets point here.
        TRef Data;
    };

    struct TBitmap
    {
        //! Bits.
        TRef Data;
    };

    struct TDictionaryEncoding
    {
        //! If |true| then dictionary indexes are in fact 1-based; 0 in #TColumn::Values means null
        //! and #TColumn::NullBitmap is not used; one should subtract 1 from value in #TColumn::Values
        //! before dereferencing the dictionary.
        bool ZeroMeansNull = false;

        //! Contains dictionary values.
        /*!
         *  Example (assuming ZeroMeansNull is true):
         *  Raw values:        hello, world, <null>, world
         *  Index values:      1, 2, 0, 2
         *  Dictionary values: hello, world
         */
        const TColumn* ValueColumn;
    };

    struct TRleEncoding
    {
        //! Contains RLE-encoded values.
        /*!
         *  Example:
         *  Raw values:  1, 1, 2, 3, 3, 3
         *  RLE values:  1, 2, 3
         *  RLE indexes: 0, 2, 3
         */
        const TColumn* ValueColumn;
    };

    struct TColumn
    {
        //! Id in name table.
        //! -1 for non-root columns.
        int Id = -1;

        //! Index of the first relevant value in the column.
        //! For non-RLE encoded columns, #Values typically provides a vector whose elements starting from #StartIndex
        //! give the desired values. For dictionary-encoded columns, these elemets are not actual values but rather
        //! dictionary indexes (see #Dictionary).
        //! For RLE encoded columns, #Values contains starting indexes of RLE segments and #StartIndex
        //! must be compared against these indexes to obtain the relavant range of values (stored in #Rle).
        i64 StartIndex = -1;

        //! The number of relevant values in this column (starting from #StartIndex).
        i64 ValueCount = -1;

        //! The type of values in this column.
        NTableClient::TLogicalTypePtr Type;
        
        //! Bitmap with ones indicating nulls.
        //! If both #NullBitmap and #Values are null then all values are null.
        //! If just #NullBitmap is null then all values are non-null.
        std::optional<TBitmap> NullBitmap;

        //! If non-null then values are actually indexes in #Dictionary.
        std::optional<TDictionaryEncoding> Dictionary;
    
        //! If non-null then continguous segments of coindicing #Values are collapsed.
        //! #Rle describes the resulting values and #Values store RLE indexes.
        std::optional<TRleEncoding> Rle;

        //! Somewhat encoded values.
        /*!
         *  Encoding proceeds as follows:
         *  1) For signed integers, zig-zag encoding could be applied (see #TValueBuffer::ZigZagEncoded)
         *  2) For integers, some base value could be subtracted (see #TValueBuffer::MinValue)
         *  3) Dictionary encoding could be applied; in this case values are replaced with integer dictionary indexes
         *  4) RLE encoding could be applied: in this case value repetitions are eliminated
         */
        std::optional<TValueBuffer> Values;

        //! Contains string data and metadata for string-like values.
        //! Null for other types.
        std::optional<TStringBuffer> Strings;
    };

    //! Returns the (root) columns representing the batch.
    //! The batch must be columnar.
    //! This call is fast.
    //! Invoking #MaterializeRows after this call is forbidden.
    virtual TRange<const TColumn*> MaterializeColumns() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedRowBatch)

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    TSharedRange<TUnversionedRow> rows);

IUnversionedRowBatchPtr CreateEmptyUnversionedRowBatch();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
