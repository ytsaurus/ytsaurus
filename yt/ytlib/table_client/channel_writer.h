#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

namespace NYT {
namespace NTableClient {

///////////////////////////////////////////////////////////////////////////////

// ToDo: move to misc
class TBlobOutput
    : public TOutputStream
{
public:
    /*!
     * \param size - size of blob reserved in ctor
     */
    TBlobOutput(size_t size);
    ~TBlobOutput() throw();

    void DoWrite(const void* buf, size_t len);

    const char* Begin() const;
    i32 GetSize() const;

    void Clear();
    TSharedRef Flush();

private:
    TBlob Blob;
};

///////////////////////////////////////////////////////////////////////////////

class TChannelWriter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChannelWriter> TPtr;

    TChannelWriter(const TChannel& channel);
    void Write(const TColumn& column, TValue value);
    void EndRow();

    size_t GetCurrentSize() const;
    int GetCurrentRowCount() const;
    bool HasUnflushedData() const;

    TSharedRef FlushBlock();

private:
    //! Size reserved for column offsets
    size_t GetEmptySize() const;

    TChannel Channel;

    //! Current buffers for fixed columns.
    yvector<TBlobOutput> FixedColumns;

    //! Current buffer for range columns.
    TBlobOutput RangeColumns;

    //! Mapping from fixed column names of the #Channel to their indexes in #FixedColumns.
    yhash_map<TColumn, int> ColumnIndexes;

    //! Is fixed column with corresponding index already set in the current row.
    yvector<bool> IsColumnUsed;

    //! Overall size of current buffers.
    size_t CurrentSize;

    //! Number of rows in the current unflushed buffer.
    int CurrentRowCount;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
