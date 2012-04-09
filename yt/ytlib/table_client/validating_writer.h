#pragma once

#include "common.h"
#include "async_writer.h"

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TValidatingWriter
{
public:
    TValidatingWriter(
        const TSchema& schema, 
        IAsyncWriter* writer);

    virtual TAsyncError AsyncOpen();
    void Write(const TColumn& column, TValue value);

    virtual TAsyncError AsyncEndRow();
    virtual TAsyncError AsyncClose();

protected:
    IAsyncWriter::TPtr Writer;
    const TSchema Schema;

    // Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TColumn, int> ColumnIndexes;

    // Used to remember set columns in current row.
    std::vector<bool> IsColumnUsed; // for columns with indexes.
    yhash_set<TColumn> UsedRangeColumns; // for columns without indexes.

    TKey CurrentKey;

    std::vector<TChannelWriter::TPtr> ChannelWriters;
    NProto::TTableChunkAttributes Attributes;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
