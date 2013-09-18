#include "stdafx.h"
#include "dsv_writer.h"

#include <ytlib/misc/error.h>

#include <yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDsvWriterBase::TDsvWriterBase(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , Table(config)
{
    YCHECK(Stream);
    YCHECK(Config);
}

void TDsvWriterBase::EscapeAndWrite(const TStringBuf& string, bool inKey)
{
    if (Config->EnableEscaping) {
        WriteEscaped(
            Stream,
            string,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol);
    } else {
        Stream->Write(string);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDsvTabularWriter::TDsvTabularWriter(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvWriterBase(stream, config)
    , State(EState::None)
    , TableIndex(0)
{ }

TDsvTabularWriter::~TDsvTabularWriter()
{ }

void TDsvTabularWriter::OnStringScalar(const TStringBuf& value)
{
    YASSERT(State == EState::ExpectColumnValue);
    EscapeAndWrite(value, false);
    State = EState::ExpectColumnName;
}

void TDsvTabularWriter::OnIntegerScalar(i64 value)
{
    if (State == EState::ExpectColumnValue) {
        Stream->Write(::ToString(value));
        State = EState::ExpectColumnName;
        return;
    }

    YASSERT(State == EState::ExpectAttributeValue);
    switch (ControlAttribute) {
        case EControlAttribute::TableIndex:
            TableIndex = value;
            break;

        default:
            YUNREACHABLE();
    }
    State = EState::ExpectAttributeName;
}

void TDsvTabularWriter::OnDoubleScalar(double value)
{
    switch (State) {
        case EState::ExpectColumnValue:
            Stream->Write(::ToString(value));
            State = EState::ExpectColumnName;
            break;

        case EState::ExpectAttributeValue:
            State = EState::ExpectAttributeName;
            break;

        case EState::None:
        case EState::ExpectAttributeName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        case EState::ExpectEntity:
        default:
            YUNREACHABLE();

    };
}

void TDsvTabularWriter::OnEntity()
{
    switch (State) {
        case EState::ExpectColumnValue:
            THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
            break;

        case EState::ExpectAttributeValue:
            State = EState::ExpectAttributeName;
            break;

        case EState::ExpectEntity:
            State = EState::None;
            break;

        case EState::None:
        case EState::ExpectAttributeName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnBeginList()
{
    switch (State) {
        case EState::ExpectColumnValue:
        case EState::ExpectAttributeValue:
            THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
            break;

        case EState::None:
        case EState::ExpectEntity:
        case EState::ExpectAttributeName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnListItem()
{
    YASSERT(State == EState::None);
}

void TDsvTabularWriter::OnEndList()
{
    YUNREACHABLE();
}

void TDsvTabularWriter::OnBeginMap()
{

    switch (State) {
        case EState::ExpectColumnValue:
        case EState::ExpectAttributeValue:
            THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
            break;

        case EState::None:
            if (Config->LinePrefix) {
                Stream->Write(Config->LinePrefix.Get());
                State = EState::ExpectColumnName;
            } else {
                State = EState::ExpectFirstColumnName;
            }
            break;

        case EState::ExpectEntity:
        case EState::ExpectAttributeName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
        case EState::ExpectAttributeName:
            ControlAttribute = ParseEnum<EControlAttribute>(ToString(key));
            State = EState::ExpectAttributeValue;
            break;

        case EState::ExpectColumnName:
            Stream->Write(Config->FieldSeparator);
            // NB: no break here!

        case EState::ExpectFirstColumnName:
            EscapeAndWrite(key, true);
            Stream->Write(Config->KeyValueSeparator);
            State = EState::ExpectColumnValue;
            break;

        case EState::None:
        case EState::ExpectEntity:
        case EState::ExpectColumnValue:
        case EState::ExpectAttributeValue:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnEndMap()
{
    if (Config->EnableTableIndex) {
        switch (State) {
            case EState::ExpectColumnName:
                Stream->Write(Config->FieldSeparator);
                // NB: no break here!

            case EState::ExpectFirstColumnName:
                EscapeAndWrite(Config->TableIndexColumn, true);
                Stream->Write(Config->KeyValueSeparator);
                Stream->Write(::ToString(TableIndex));
                break;

            case EState::None:
            case EState::ExpectAttributeName:
            case EState::ExpectAttributeValue:
            case EState::ExpectEntity:
            case EState::ExpectColumnValue:
                YUNREACHABLE();
        }
    }
    State = EState::None;
    Stream->Write(Config->RecordSeparator);
}

void TDsvTabularWriter::OnBeginAttributes()
{
    switch (State) {
        case EState::ExpectAttributeValue:
        case EState::ExpectColumnValue:
            THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
            break;

        case EState::None:
            State = EState::ExpectAttributeName;
            break;

        case EState::ExpectAttributeName:
        case EState::ExpectColumnName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectEntity:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnEndAttributes()
{
    YASSERT(State == EState::ExpectAttributeName);
    State = EState::ExpectEntity;
}

////////////////////////////////////////////////////////////////////////////////

TDsvNodeWriter::TDsvNodeWriter(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvWriterBase(stream, config)
    , AllowBeginList(true)
    , AllowBeginMap(true)
    , BeforeFirstMapItem(true)
    , BeforeFirstListItem(true)
{ }

TDsvNodeWriter::~TDsvNodeWriter()
{ }

void TDsvNodeWriter::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, false);
}

void TDsvNodeWriter::OnIntegerScalar(i64 value)
{
    Stream->Write(::ToString(value));
}

void TDsvNodeWriter::OnDoubleScalar(double value)
{
    Stream->Write(::ToString(value));
}

void TDsvNodeWriter::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
}

void TDsvNodeWriter::OnBeginList()
{
    if (AllowBeginList) {
        AllowBeginList = false;
    } else {
        THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
    }
}

void TDsvNodeWriter::OnListItem()
{
    AllowBeginMap = true;
    if (BeforeFirstListItem) {
        BeforeFirstListItem = false;
    } else {
        // Not first item.
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvNodeWriter::OnEndList()
{
    Stream->Write(Config->RecordSeparator);
}

void TDsvNodeWriter::OnBeginMap()
{
    if (AllowBeginMap) {
        AllowBeginList = false;
        AllowBeginMap = false;
        BeforeFirstMapItem = true;
    } else {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
}

void TDsvNodeWriter::OnKeyedItem(const TStringBuf& key)
{
    YASSERT(!AllowBeginMap);
    YASSERT(!AllowBeginList);

    if (BeforeFirstMapItem) {
        BeforeFirstMapItem = false;
    } else {
        Stream->Write(Config->FieldSeparator);
    }

    EscapeAndWrite(key, true);
    Stream->Write(Config->KeyValueSeparator);
}

void TDsvNodeWriter::OnEndMap()
{
    YASSERT(!AllowBeginMap);
    YASSERT(!AllowBeginList);
}

void TDsvNodeWriter::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
}

void TDsvNodeWriter::OnEndAttributes()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
