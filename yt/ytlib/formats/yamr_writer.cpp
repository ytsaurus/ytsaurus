#include "stdafx.h"
#include "yamr_writer.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrWriter::TYamrWriter(TOutputStream* stream, TYamrFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , AllowBeginMap(true)
    , State(EState::None)
{
    if (!Config) {
        Config = New<TYamrFormatConfig>();
    }
}

TYamrWriter::~TYamrWriter()
{ }

void TYamrWriter::OnIntegerScalar(i64 value)
{
    RememberItem(ToString(value), true);
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

    Key.Clear();
    Subkey.Clear();
    Value.Clear();
}

void TYamrWriter::OnKeyedItem(const TStringBuf& key)
{
    if (key == Config->Key) {
        State = EState::ExpectingKey;
    } else if (Config->HasSubkey && key == Config->Subkey) {
        State = EState::ExpectingSubkey;
    } else if (key == Config->Value) {
        State = EState::ExpectingValue;
    }
}

void TYamrWriter::OnEndMap()
{
    AllowBeginMap = true;
    WriteRow();
}

void TYamrWriter::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Attributes are not supported by YAMR");
}

void TYamrWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

void TYamrWriter::RememberItem(const TStringBuf& item, bool takeOwnership)
{
    TStringBuf* value;
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
        *value = TStringBuf(buffer->Begin(), buffer->GetSize());
    } else {
        *value = item;
    }
}

void TYamrWriter::WriteRow()
{
    if (!Config->Lenval) {
        Stream->Write(Key);
        Stream->Write(Config->FieldSeparator);
        if (Config->HasSubkey) {
            Stream->Write(Subkey);
            Stream->Write(Config->FieldSeparator);
        }
        Stream->Write(Value);
        Stream->Write(Config->RecordSeparator);
    } else {
        WriteInLenvalMode(Key);
        if (Config->HasSubkey) {
            WriteInLenvalMode(Subkey);
        }
        WriteInLenvalMode(Value);
    }
}

void TYamrWriter::WriteInLenvalMode(const TStringBuf& value)
{
    WritePod(*Stream, static_cast<i32>(value.size()));
    Stream->Write(value);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
