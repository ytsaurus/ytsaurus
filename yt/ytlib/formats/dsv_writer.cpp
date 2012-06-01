#include "stdafx.h"
#include "dsv_writer.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TDsvWriter::TDsvWriter(TOutputStream* stream, TDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , FirstLine(true)
    , FirstItem(true)
    , State(EState::ExpectListItem)
{
    if (!Config) {
        Config = New<TDsvFormatConfig>();
    }
    EscapedSymbols[0] = Config->EscapingSymbol;
    EscapedSymbols[1] = Config->KeyValueSeparator;
    EscapedSymbols[2] = Config->FieldSeparator;
    EscapedSymbols[3] = Config->RecordSeparator;
}

void TDsvWriter::OnStringScalar(const TStringBuf& value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    EscapeAndWrite(value);
}

void TDsvWriter::OnIntegerScalar(i64 value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TDsvWriter::OnDoubleScalar(double value)
{
    if (State != EState::AfterKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TDsvWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported";
}

void TDsvWriter::OnBeginList()
{
    ythrow yexception() << "Lists are not supported";
}

void TDsvWriter::OnListItem()
{
    if (State != EState::ExpectListItem) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectBeginMap;

    if (!FirstLine) {
        Stream->Write(Config->RecordSeparator);
    }
    FirstItem = true;
    FirstLine = false;
}

void TDsvWriter::OnEndList()
{
    ythrow yexception() << "Lists are not supported";
}

void TDsvWriter::OnBeginMap()
{
    if (State != EState::ExpectBeginMap) {
        ythrow yexception() << "Unexpected call";
    }
    if (Config->LinePrefix) {
        Stream->Write(Config->LinePrefix.Get());
    }

    State = EState::ExpectKey;
}

void TDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    if (State != EState::ExpectKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::AfterKey;

    if (!FirstItem || Config->LinePrefix) {
        Stream->Write(Config->FieldSeparator);
    }

    EscapeAndWrite(key);
    Stream->Write(Config->KeyValueSeparator);
    FirstItem = false;
}

void TDsvWriter::OnEndMap()
{
    if (State != EState::ExpectKey) {
        ythrow yexception() << "Unexpected call";
    }
    State = EState::ExpectListItem;
}

void TDsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TDsvWriter::OnEndAttributes()
{
    ythrow yexception() << "Attributes are not supported";
}

void TDsvWriter::EscapeAndWrite(const TStringBuf& key)
{
    auto current = key.begin();
    auto end = key.end();
    while (current != end) {
        auto next = std::find_first_of(
            current, end,
            EscapedSymbols, EscapedSymbols + ARRAY_SIZE(EscapedSymbols));
        Stream->Write(current, next - current);
        if (next != end) {
            Stream->Write(Config->EscapingSymbol);
            Stream->Write(*next);
            ++next;
        }
        current = next;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
