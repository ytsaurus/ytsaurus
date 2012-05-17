#pragma once

#include "public.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/blob_range.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    explicit TTableConsumer(const ISyncWriterPtr& writer);

private:
    void OnMyStringScalar(const TStringBuf& value);
    void OnMyIntegerScalar(i64 value);
    void OnMyDoubleScalar(double value);
    void OnMyEntity();
    void OnMyBeginList();
    void OnMyListItem();
    void OnMyBeginMap();
    void OnMyKeyedItem(const TStringBuf& name);
    void OnMyEndMap();

    // XXX(psushin): We currently ignore user attributes.
    // void OnMyBeginAttributes();

    void OnValueFinished();

    void ThrowMapExpected();

    ISyncWriterPtr Writer;

    //! Names of key columns.
    TNullable<TKeyColumns> KeyColumns;

    //! Name-to-index map for #KeyColumns. Actual names are kept in #KeyColumns.
    yhash_map<TStringBuf, int> KeyColumnToIndex;

    bool InsideRow;

    //! Names of columns seen in the currently filled row.
    yhash_set<TBlobRange> UsedColumns;

    //! Keeps the current row data.
    TBlobOutput RowBuffer;

    //! |(endColumn, endValue)| offsets in #RowBuffer.
    std::vector<size_t> Offsets;

    NYTree::TYsonWriter ValueConsumer;

    //! A cached callback for #OnValueFinished.
    TClosure OnValueFinished_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
