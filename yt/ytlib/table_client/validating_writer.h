#pragma once
#include "common.h"
#include "writer.h"

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TValidatingWriter
{
public:
    TValidatingWriter(
        const TSchema& schema, 
        const std::vector<TColumn>& keyColumns,
        IAsyncWriter* writer);

    virtual TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);

    virtual TAsyncError::TPtr AsyncEndRow();
    virtual TAsyncError::TPtr AsyncClose();

protected:
    IAsyncWriter::TPtr Writer;
    const TSchema Schema;

    // Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TColumn, int> ColumnIndexes;
    const int KeyColumnsCount;

    // Used to remember set columns in current row.
    std::vector<bool> IsColumnUsed; // for columns with indexes.
    yhash_set<TColumn> UsedRangeColumns; // for columns without indexes.

    std::vector< TNullable<Stroka> > CurrentKey;
    bool RowStart;

    std::vector<TChannelWriter::TPtr> ChannelWriters;
    NProto::TTableChunkAttributes Attributes;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
