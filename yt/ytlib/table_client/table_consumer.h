#pragma once

#include "public.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/lexer.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/blob_range.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  For performance reasons we don't use ForwardingConsumer.
 */
class TTableConsumer
    : public NYTree::IYsonConsumer
{
public:
    TTableConsumer(const ISyncWriterPtr& writer);

private:
    void OnStringScalar(const TStringBuf& value);
    void OnIntegerScalar(i64 value);
    void OnDoubleScalar(double value);
    void OnEntity();
    void OnBeginList();
    void OnListItem();
    void OnBeginMap();
    void OnKeyedItem(const TStringBuf& name);
    void OnEndMap();

    void OnBeginAttributes();

    void ThrowMapExpected();

    void OnEndList();
    void OnEndAttributes();
    void OnRaw(const TStringBuf& yson, NYTree::EYsonType type);

    ISyncWriterPtr Writer;

    int Depth;

    //! Keeps the current row data.
    TBlobOutput RowBuffer;

    //! |(endColumn, endValue)| offsets in #RowBuffer.
    std::vector<size_t> Offsets;

    NYTree::TYsonWriter ValueWriter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
