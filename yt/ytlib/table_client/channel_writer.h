#pragma once

#include "public.h"
#include "schema.h"

#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(int fixedColumnCount);

    void WriteFixed(int fixedIndex, const TStringBuf& value);
    void WriteRange(const TStringBuf& name, const TStringBuf& value);
    void WriteRange(int chunkColumnIndex, const TStringBuf& value);

    void EndRow();

    size_t GetCurrentSize() const;

    //! Number of rows in the current unflushed buffer.
    int GetCurrentRowCount() const;

    std::vector<TSharedRef> FlushBlock();

private:
    //! Size reserved for column offsets
    size_t GetEmptySize() const;

    //! Current buffers for fixed columns.
    std::vector<TBlobOutput> FixedColumns;

    //! Current buffer for range columns.
    TBlobOutput RangeColumns;

    //! Is fixed column with corresponding index already set in the current row.
    std::vector<bool> IsColumnUsed;

    //! Overall size of current buffers.
    size_t CurrentSize;

    //! Number of rows in the current unflushed buffer.
    int CurrentRowCount;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
