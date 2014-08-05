#include "stdafx.h"
#include "yamr_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TYamrConsumer::TYamrConsumer(TOutputStream* stream, TYamrFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , Table(
        Config->FieldSeparator,
        Config->RecordSeparator,
        Config->EnableEscaping, // Enable key escaping
        Config->EnableEscaping, // Enable value escaping
        Config->EscapingSymbol,
        true)
    , State(EState::None)
{
    YCHECK(Config);
    YCHECK(Stream);
}

TYamrConsumer::~TYamrConsumer()
{ }

void TYamrConsumer::OnInt64Scalar(i64 value)
{
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }

    YASSERT(State == EState::ExpectAttributeValue);

    switch (ControlAttribute) {
    case EControlAttribute::TableIndex:
        if (!Config->EnableTableIndex) {
            // Silently ignore table switches.
            break;
        }

        if (Config->Lenval) {
            WritePod(*Stream, static_cast<ui32>(-1));
            WritePod(*Stream, static_cast<ui32>(value));
        } else {
            Stream->Write(ToString(value));
            Stream->Write(Config->RecordSeparator);
        }
        break;

    default:
        YUNREACHABLE();
    }

    State = EState::ExpectEndAttributes;
}

void TYamrConsumer::OnUint64Scalar(ui64 value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }
    THROW_ERROR_EXCEPTION("Uint64 attributes are not supported by YAMR");
}

void TYamrConsumer::OnDoubleScalar(double value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(::ToString(value));
        OnStringScalar(StringStorage_.back());
        return;
    }
    THROW_ERROR_EXCEPTION("Double attributes are not supported by YAMR");
}

void TYamrConsumer::OnBooleanScalar(bool value)
{
    YASSERT(State == EState::ExpectValue || State == EState::ExpectAttributeValue);
    if (State == EState::ExpectValue) {
        StringStorage_.push_back(Stroka(FormatBool(value)));
        OnStringScalar(StringStorage_.back());
        return;
    }
    THROW_ERROR_EXCEPTION("Boolean attributes are not supported by YAMR");
}

void TYamrConsumer::OnStringScalar(const TStringBuf& value)
{
    YCHECK(State != EState::ExpectAttributeValue);
    YASSERT(State == EState::ExpectValue);

    switch (ValueType) {
        case EValueType::ExpectKey:
            Key = value;
            break;

        case EValueType::ExpectSubkey:
            Subkey = value;
            break;

        case EValueType::ExpectValue:
            Value = value;
            break;

        case EValueType::ExpectUnknown:
            //Ignore unknows columns.
            break;

        default:
            YUNREACHABLE();
    }

    State = EState::ExpectColumnName;
}

void TYamrConsumer::OnEntity()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Entities are not supported by YAMR");
    }

    YASSERT(State == EState::ExpectEntity);
    State = EState::None;
}

void TYamrConsumer::OnBeginList()
{
    YASSERT(State == EState::ExpectValue);
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMR");
}

void TYamrConsumer::OnListItem()
{
    YASSERT(State == EState::None);
}

void TYamrConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TYamrConsumer::OnBeginMap()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMR");
    }
    YASSERT(State == EState::None);
    State = EState::ExpectColumnName;

    Key = Null;
    Subkey = Null;
    Value = Null;
}

void TYamrConsumer::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
    case EState::ExpectColumnName:
        if (key == Config->Key) {
            ValueType = EValueType::ExpectKey;
        } else if (key == Config->Subkey) {
            ValueType = EValueType::ExpectSubkey;
        } else if (key == Config->Value) {
            ValueType = EValueType::ExpectValue;
        } else {
            ValueType = EValueType::ExpectUnknown;
        }

        State = EState::ExpectValue;
        break;

    case EState::ExpectAttributeName:
        ControlAttribute = ParseEnum<EControlAttribute>(ToString(key));
        State = EState::ExpectAttributeValue;
        break;

    case EState::None:
    case EState::ExpectValue:
    case EState::ExpectAttributeValue:
    case EState::ExpectEntity:
    case EState::ExpectEndAttributes:
    default:
        YUNREACHABLE();
    }
}

void TYamrConsumer::OnEndMap()
{
    YASSERT(State == EState::ExpectColumnName);
    State = EState::None;

    WriteRow();
}

void TYamrConsumer::OnBeginAttributes()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMR");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectAttributeName;
}

void TYamrConsumer::OnEndAttributes()
{
    YASSERT(State == EState::ExpectEndAttributes);
    State = EState::ExpectEntity;
}

void TYamrConsumer::WriteRow()
{
    if (!Key) {
        THROW_ERROR_EXCEPTION("Missing column %Qv in YAMR record",
            Config->Key);
    }

    if (!Value) {
        THROW_ERROR_EXCEPTION("Missing column %Qv in YAMR record",
            Config->Value);
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

void TYamrConsumer::WriteInLenvalMode(const TStringBuf& value)
{
    WritePod(*Stream, static_cast<ui32>(value.size()));
    Stream->Write(value);
}

void TYamrConsumer::EscapeAndWrite(const TStringBuf& value, bool inKey)
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
