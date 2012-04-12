#pragma once

#include "public.h"
#include "value_consumer.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
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
*/

////////////////////////////////////////////////////////////////////////////////

class TTableConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    TTableConsumer(const ISyncWriterPtr& writer);

private:
    void OnMyStringScalar(const TStringBuf& value, bool hasAttributes);
    void OnMyIntegerScalar(i64 value, bool hasAttributes);
    void OnMyDoubleScalar(double value, bool hasAttributes);
    void OnMyEntity(bool hasAttributes);
    void OnMyBeginList();
    void OnMyListItem();
    void OnMyBeginMap();
    void OnMyMapItem(const TStringBuf& name);
    void OnMyEndMap(bool hasAttributes);

    void OnValueEnded();

    void CheckNoAttributes(bool hasAttributes);

    virtual void OnColumn();

    ISyncWriterPtr Writer;
    TNullable<TKeyColumns> KeyColumns;

    bool InsideRow;

    size_t ValueOffset;
    TStringBuf CurrentColumn;

    TKey CurrentKey;
    TKey PreviousKey;

    yhash_set<TStringBuf> UsedColumns;

    TRow Row;
    TBlobOutput RowBuffer;
    TValueConsumer ValueConsumer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
