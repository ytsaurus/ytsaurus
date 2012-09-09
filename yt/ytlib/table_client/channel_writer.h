#pragma once

#include "public.h"
#include "schema.h"

#include <ytlib/misc/chunked_output_stream.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCounted
{
    DEFINE_BYVAL_RW_PROPERTY(int, HeapIndex);
    DEFINE_BYVAL_RO_PROPERTY(int, BufferIndex);

public:
    static const int MaxReserveSize;

    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(
        int bufferIndex,
        int fixedColumnCount,
        bool writeRangeSizes = false);

    void WriteFixed(int fixedIndex, const TStringBuf& value);
    void WriteRange(const TStringBuf& name, const TStringBuf& value);
    void WriteRange(int chunkColumnIndex, const TStringBuf& value);

    void EndRow();

    size_t GetCurrentSize() const;
    size_t GetCapacity() const;

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

    //! Is fixed column with corresponding index already set in the current row.
    std::vector<bool> IsColumnUsed;

    //! Total size of data in buffers.
    size_t CurrentSize;

    //! Total size of reserved buffers.
    size_t Capacity;

    //! Number of rows in the current unflushed buffer.
    int CurrentRowCount;

    bool WriteRangeSizes;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
