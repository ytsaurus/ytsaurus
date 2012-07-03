#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "key.h"
#include "config.h"

namespace NYT {
namespace NTableClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const ISyncWriterPtr& writer)
    : Writer(writer)
    , Depth(0)
    , ValueWriter(&RowBuffer)
{ }

void TTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TTableConsumer::OnIntegerScalar(i64 value)
{
    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnIntegerScalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnDoubleScalar(value);
    }
}

void TTableConsumer::OnEntity()
{
    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnEntity();
    }
}

void TTableConsumer::OnBeginList()
{
    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TTableConsumer::OnBeginAttributes()
{
    if (Depth == 0) {
        ythrow yexception() <<
            Sprintf("Invalid row format, attributes are not supported (RowIndex: %"PRId64")", Writer->GetRowCount());
    } else {
        ++Depth;
        ValueWriter.OnBeginAttributes();
    }
}

void TTableConsumer::ThrowMapExpected()
{
    ythrow yexception() << Sprintf(
        "Invalid row format, map expected (RowIndex: %" PRId64 ")",
        Writer->GetRowCount());
}

void TTableConsumer::OnListItem()
{
    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (Depth > 0) {
        ValueWriter.OnBeginMap();
    }

    ++Depth;
}

void TTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    YASSERT(Depth > 0);
    if (Depth == 1) {

        Offsets.push_back(RowBuffer.GetSize());
        RowBuffer.Write(name);

        Offsets.push_back(RowBuffer.GetSize());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YASSERT(Depth > 0);

    --Depth;

    if (Depth > 0) {
        ValueWriter.OnEndMap();
        return;
    }

    YASSERT(Offsets.size() % 2 == 0);

    TRow row;
    row.reserve(Offsets.size() / 2);

    int index = Offsets.size();
    int begin = RowBuffer.GetSize();
    while (index > 0) {
        int end = begin;
        begin = Offsets[--index];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);

        end = begin;
        begin = Offsets[--index];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);

        row.push_back(std::make_pair(name, value));
    }

    Writer->WriteRow(row);

    Offsets.clear();
    RowBuffer.Clear();
}

void TTableConsumer::OnEndList()
{
    --Depth;
    YASSERT(Depth > 0);
    ValueWriter.OnEndList();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth;
    YASSERT(Depth > 0);
    ValueWriter.OnEndList();
}

void TTableConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
