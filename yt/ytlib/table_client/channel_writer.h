#pragma once

#include "public.h"
#include "schema.h"

#include <ytlib/misc/blob_output.h>
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
    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(
        int bufferIndex,
        int fixedColumnCount,
        bool writeRangeSizes = false);

    int WriteFixed(int fixedIndex, const TStringBuf& value);
    int WriteRange(const TStringBuf& name, const TStringBuf& value);
    int WriteRange(int chunkColumnIndex, const TStringBuf& value);

    int EndRow();

    size_t GetCurrentSize() const;

    //! Number of rows in the current unflushed buffer.
    i64 GetCurrentRowCount() const;

    std::vector<TSharedRef> FlushBlock();

private:
    //! Current buffers for fixed columns.
    std::vector<TBlobOutput> FixedColumns;

    //! Current buffer for range columns.
    TBlobOutput RangeColumns;

    TBlobOutput RangeSizes;
    int RangeOffset;

    //! Is fixed column with corresponding index already set in the current row.
    std::vector<bool> IsColumnUsed;

    //! Overall size of current buffers.
    size_t CurrentSize;

    //! Number of rows in the current unflushed buffer.
    int CurrentRowCount;

    bool WriteRangeSizes;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
