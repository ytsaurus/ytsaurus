#include "stdafx.h"
#include "dsv_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;

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
{ }

TDsvTabularWriter::~TDsvTabularWriter()
{ }

void TDsvTabularWriter::OnStringScalar(const TStringBuf& value)
{
    switch (State) {
        case EState::ExpectColumnValue:
            EscapeAndWrite(value, false);
            State = EState::ExpectColumnName;
            break;

        case EState::InsideAttributes:
            break;

        case EState::None:
        case EState::ExpectColumnName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectEntity:
        default:
            YUNREACHABLE();

    };
}

void TDsvTabularWriter::OnIntegerScalar(i64 value)
{
    switch (State) {
        case EState::ExpectColumnValue:
            Stream->Write(::ToString(value));
            State = EState::ExpectColumnName;
            break;

        case EState::InsideAttributes:
            break;

        case EState::None:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        case EState::ExpectEntity:
        default:
            YUNREACHABLE();

    };
}

void TDsvTabularWriter::OnDoubleScalar(double value)
{
    switch (State) {
        case EState::ExpectColumnValue:
            Stream->Write(::ToString(value));
            State = EState::ExpectColumnName;
            break;

        case EState::InsideAttributes:
            break;

        case EState::None:
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

        case EState::InsideAttributes:
            break;

        case EState::ExpectEntity:
            State = EState::None;
            break;

        case EState::None:
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
        case EState::InsideAttributes:
            THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
            break;

        case EState::None:
        case EState::ExpectEntity:
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
        case EState::InsideAttributes:
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
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
        case EState::InsideAttributes:
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
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnEndMap()
{
    YASSERT(State == EState::ExpectColumnName || State == EState::ExpectFirstColumnName);
    State = EState::None;
    Stream->Write(Config->RecordSeparator);
}

void TDsvTabularWriter::OnBeginAttributes()
{
    switch (State) {
        case EState::InsideAttributes:
        case EState::ExpectColumnValue:
            THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
            break;

        case EState::None:
            State = EState::InsideAttributes;
            break;

        case EState::ExpectColumnName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectEntity:
        default:
            YUNREACHABLE();
    };
}

void TDsvTabularWriter::OnEndAttributes()
{
    YASSERT(State == EState::InsideAttributes);
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
