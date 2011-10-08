#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(const TChannel& channel);
    void Write(TColumn column, TValue value);
    void EndRow();
    size_t GetCurrentSize() const;
    bool HasUnflushedData() const;
    TSharedRef FlushBlock();

private:
    size_t GetEmptySize() const;
    void AppendSize(ui32 size, TBlob* data);
    void AppendValue(TValue value, TBlob* data);

    TChannel Channel;

    //! Current buffers for fixed columns.
    yvector<TBlob> FixedColumns;

    //! Current buffer for range columns.
    TBlob RangeColumns;

    //! Mapping from fixed column names of the #Channel to their indexes in #FixedColumns.
    yhash_map<TColumn, int> ColumnIndexes;

    //! Is fixed column with corresponding index already set in the current row.
    yvector<bool> ColumnSetFlags;

    //! Overall size of current buffers.
    size_t CurrentSize;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
