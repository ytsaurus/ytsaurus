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
    YCHECK(State == EState::AfterKey);
    State = EState::ExpectKey;
    EscapeAndWrite(value);
}

void TDsvWriter::OnIntegerScalar(i64 value)
{
    YCHECK(State == EState::AfterKey);
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TDsvWriter::OnDoubleScalar(double value)
{
    YCHECK(State == EState::AfterKey);
    State = EState::ExpectKey;
    Stream->Write(ToString(value));
}

void TDsvWriter::OnEntity()
{
    ythrow yexception() << "Entities are not supported by dsv";
}

void TDsvWriter::OnBeginList()
{
    ythrow yexception() << "Embedded lists are not supported by dsv";
}

void TDsvWriter::OnListItem()
{
    YCHECK(State == EState::ExpectListItem);
    State = EState::ExpectBeginMap;

    if (!FirstLine) {
        Stream->Write(Config->RecordSeparator);
    }
    FirstItem = true;
    FirstLine = false;
}

void TDsvWriter::OnEndList()
{
    YUNREACHABLE();
}

void TDsvWriter::OnBeginMap()
{
    if (State != EState::ExpectBeginMap) {
        ythrow yexception() << "Embedded maps are not supported by dsv";
    }
    if (Config->LinePrefix) {
        Stream->Write(Config->LinePrefix.Get());
    }

    State = EState::ExpectKey;
}

void TDsvWriter::OnKeyedItem(const TStringBuf& key)
{
    YCHECK(State == EState::ExpectKey);
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
    YCHECK(State == EState::ExpectKey);
    State = EState::ExpectListItem;
}

void TDsvWriter::OnBeginAttributes()
{
    ythrow yexception() << "Attributes are not supported by dsv";
}

void TDsvWriter::OnEndAttributes()
{
    YUNREACHABLE();
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
