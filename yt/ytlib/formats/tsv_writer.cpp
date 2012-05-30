#include "stdafx.h"
#include "tsv_writer.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TTsvWriter::TTsvWriter(TOutputStream* stream, TTsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , FirstLine(true)
    , FirstItem(true)
    , State(EState::ExpectListItem)
{
    YCHECK(Config);
}

void TTsvWriter::OnStringScalar(const TStringBuf& value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    Stream->Write(value);
}

void TTsvWriter::OnIntegerScalar(i64 value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TTsvWriter::OnDoubleScalar(double value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TTsvWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported";
}

void TTsvWriter::OnBeginList()
{
    ythrow yexception() << "Lists are not supported";
}

void TTsvWriter::OnListItem()
{
    if (State != EState::ExpectListItem) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectBeginMap;

    if (!FirstLine) {
        Stream->Write(Config->NewLineSeparator);
    }
    FirstItem = true;
    FirstLine = false;
}

void TTsvWriter::OnEndList()
{
    ythrow yexception() << "Lists are not supported";
}

void TTsvWriter::OnBeginMap()
{
    if (State != EState::ExpectBeginMap) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
}

void TTsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (State != EState::ExpectKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::AfterKey;

    if (!FirstItem) {
        Stream->Write(Config->ItemSeparator);
    }
    Stream->Write(key);
    Stream->Write(Config->KeyValueSeparator);
    FirstItem = false;
}

void TTsvWriter::OnEndMap()
{
    if (State != EState::ExpectKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectListItem;
}

void TTsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TTsvWriter::OnEndAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
