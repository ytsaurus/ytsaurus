#pragma once

#include "public.h"

#include <core/misc/chunked_output_stream.h>
#include <core/misc/property.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(int, BufferIndex);
    DEFINE_BYVAL_RW_PROPERTY(int, HeapIndex);

public:
    static const int MaxUpperReserveLimit;
    static const int MinUpperReserveLimit;

    TChannelWriter(
        int bufferIndex,
        int fixedColumnCount,
        bool writeRangeSizes = false,
        int upperReserveLimit = MaxUpperReserveLimit);

    void WriteFixed(int fixedIndex, const TStringBuf& value);
    void WriteRange(const TStringBuf& name, const TStringBuf& value);
    void WriteRange(int chunkColumnIndex, const TStringBuf& value);

    void EndRow();

    i64 GetDataSize() const;
    i64 GetCapacity() const;

    //! Number of rows in the current unflushed buffer.
    i64 GetCurrentRowCount() const;

    std::vector<TSharedRef> FlushBlock();

private:
    void InitCapacity();

    //! Current buffers for fixed columns.
    std::vector<TChunkedOutputStream> FixedColumns;

    //! Current buffer for range columns.
    TChunkedOutputStream RangeColumns;

    TChunkedOutputStream RangeSizes;
    int RangeOffset;
    bool WriteRangeSizes;

    //! Is fixed column with corresponding index already set in the current row.
    std::vector<bool> IsColumnUsed;

    //! Total size of data in buffers.
    i64 CurrentSize;

    //! Total size of reserved buffers.
    i64 Capacity;

    //! Number of rows in the current unflushed buffer.
    int CurrentRowCount;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
