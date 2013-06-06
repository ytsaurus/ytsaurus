#include "stdafx.h"
#include "yamr_writer.h"

#include <ytlib/misc/error.h>

#include <ytlib/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrWriter::TYamrWriter(TOutputStream* stream, TYamrFormatConfigPtr config)
    : Stream(stream)
    , Config(config ? config : New<TYamrFormatConfig>())
    , Table(
        Config->FieldSeparator,
        Config->RecordSeparator,
        Config->EnableEscaping, // Enable key escaping
        Config->EnableEscaping, // Enable value escaping
        Config->EscapingSymbol, 
        Config->EscapeCarriageReturn,
        true)
    , AllowBeginMap(true)
    , ExpectTableIndex(false)
    , State(EState::None)
{ }

TYamrWriter::~TYamrWriter()
{ }

void TYamrWriter::OnIntegerScalar(i64 value)
{
    if (!ExpectTableIndex) {
        RememberItem(ToString(value), true);
    } else {
        WritePod(*Stream, static_cast<i16>(value));
        ExpectTableIndex = false;
    }
}

void TYamrWriter::OnDoubleScalar(double value)
{
    RememberItem(ToString(value), true);
}

void TYamrWriter::OnStringScalar(const TStringBuf& value)
{
    RememberItem(value, false);
}

void TYamrWriter::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by YAMR");
}

void TYamrWriter::OnBeginList()
{
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMR");
}

void TYamrWriter::OnListItem()
{ }

void TYamrWriter::OnEndList()
{
    YUNREACHABLE();
}

void TYamrWriter::OnBeginMap()
{
    if (!AllowBeginMap) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMR");
    }
    AllowBeginMap = false;

    Key = Null;
    Subkey = Null;
    Value = Null;
}

void TYamrWriter::OnKeyedItem(const TStringBuf& key)
{
    if (key == Config->Key) {
        State = EState::ExpectingKey;
    } else if (Config->HasSubkey && key == Config->Subkey) {
        State = EState::ExpectingSubkey;
    } else if (key == Config->Value) {
        State = EState::ExpectingValue;
    } else if (key == "table_index") {
        ExpectTableIndex = true;
    }
}

void TYamrWriter::OnEndMap()
{
    AllowBeginMap = true;
    WriteRow();
}

void TYamrWriter::OnBeginAttributes()
{
    if (!Config->EnableTableIndex) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMR");
    }
}

void TYamrWriter::OnEndAttributes()
{
    if (!Config->EnableTableIndex) {
        YUNREACHABLE();
    }
}

void TYamrWriter::RememberItem(const TStringBuf& item, bool takeOwnership)
{
    TNullable<TStringBuf>* value;
    TBlobOutput* buffer;

    switch (State) {
        case EState::None:
            return;
        case EState::ExpectingKey:
            value = &Key;
            buffer = &KeyBuffer;
            break;
        case EState::ExpectingSubkey:
            value = &Subkey;
            buffer = &SubkeyBuffer;
            break;
        case EState::ExpectingValue:
            value = &Value;
            buffer = &ValueBuffer;
            break;
        default:
            YUNREACHABLE();
    }
    State = EState::None;

    if (takeOwnership) {
        buffer->Clear();
        buffer->PutData(item);
        value->Assign(buffer->Blob().ToStringBuf());
    } else {
        value->Assign(item);
    }
}

void TYamrWriter::WriteRow()
{
    if (!Key) {
        THROW_ERROR_EXCEPTION("Column %s is missing in YAMR record", ~Config->Key);
    }

    if (!Value) {
        THROW_ERROR_EXCEPTION("Column %s is missing in YAMR record", ~Config->Value);
    }

    TStringBuf key = *Key;
    TStringBuf subkey = Subkey ? *Subkey : "";
    TStringBuf value = *Value;

    if (!Config->Lenval) {
        EscapeAndWrite(key, true);
        Stream->Write(Config->FieldSeparator);
        if (Config->HasSubkey) {
            EscapeAndWrite(subkey, true);
            Stream->Write(Config->FieldSeparator);
        }
        EscapeAndWrite(value, false);
        Stream->Write(Config->RecordSeparator);
    } else {
        WriteInLenvalMode(key);
        if (Config->HasSubkey) {
            WriteInLenvalMode(subkey);
        }
        WriteInLenvalMode(value);
    }
}

void TYamrWriter::WriteInLenvalMode(const TStringBuf& value)
{
    WritePod(*Stream, static_cast<i32>(value.size()));
    Stream->Write(value);
}

void TYamrWriter::EscapeAndWrite(const TStringBuf& value, bool inKey)
{
    if (Config->EnableEscaping) {
        WriteEscaped(
            Stream,
            value,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol);
    } else {
        Stream->Write(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
