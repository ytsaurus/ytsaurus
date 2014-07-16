#include "stdafx.h"
#include "dsv_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDsvConsumerBase::TDsvConsumerBase(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , Table(config, true)
{
    YCHECK(Stream);
    YCHECK(Config);
}

void TDsvConsumerBase::EscapeAndWrite(const TStringBuf& string, bool inKey)
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

TDsvTabularConsumer::TDsvTabularConsumer(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvConsumerBase(stream, config)
    , State(EState::None)
    , TableIndex(0)
{ }

TDsvTabularConsumer::~TDsvTabularConsumer()
{ }

void TDsvTabularConsumer::OnStringScalar(const TStringBuf& value)
{
    YASSERT(State == EState::ExpectColumnValue);
    EscapeAndWrite(value, false);
    State = EState::ExpectColumnName;
}

void TDsvTabularConsumer::OnInt64Scalar(i64 value)
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

void TDsvTabularConsumer::OnDoubleScalar(double value)
{
    switch (State) {
        case EState::ExpectColumnValue:
            Stream->Write(::ToString(value));
            State = EState::ExpectColumnName;
            break;

        case EState::None:
        case EState::ExpectAttributeName:
        case EState::ExpectFirstColumnName:
        case EState::ExpectColumnName:
        case EState::ExpectEntity:
        case EState::ExpectAttributeValue:
        default:
            YUNREACHABLE();

    };
}

void TDsvTabularConsumer::OnBooleanScalar(bool value)
{
    if (State == EState::ExpectColumnValue) {
        Stream->Write(FormatBool(value));
        State = EState::ExpectColumnName;
        return;
    }

    YUNREACHABLE();
}

void TDsvTabularConsumer::OnEntity()
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

void TDsvTabularConsumer::OnBeginList()
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

void TDsvTabularConsumer::OnListItem()
{
    YASSERT(State == EState::None);
}

void TDsvTabularConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TDsvTabularConsumer::OnBeginMap()
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

void TDsvTabularConsumer::OnKeyedItem(const TStringBuf& key)
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

void TDsvTabularConsumer::OnEndMap()
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

void TDsvTabularConsumer::OnBeginAttributes()
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

void TDsvTabularConsumer::OnEndAttributes()
{
    YASSERT(State == EState::ExpectAttributeName);
    State = EState::ExpectEntity;
}

////////////////////////////////////////////////////////////////////////////////

TDsvNodeConsumer::TDsvNodeConsumer(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvConsumerBase(stream, config)
    , AllowBeginList(true)
    , AllowBeginMap(true)
    , BeforeFirstMapItem(true)
    , BeforeFirstListItem(true)
{ }

TDsvNodeConsumer::~TDsvNodeConsumer()
{ }

void TDsvNodeConsumer::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, false);
}

void TDsvNodeConsumer::OnInt64Scalar(i64 value)
{
    Stream->Write(::ToString(value));
}

void TDsvNodeConsumer::OnDoubleScalar(double value)
{
    Stream->Write(::ToString(value));
}

void TDsvNodeConsumer::OnBooleanScalar(bool value)
{
    Stream->Write(FormatBool(value));
}

void TDsvNodeConsumer::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
}

void TDsvNodeConsumer::OnBeginList()
{
    if (AllowBeginList) {
        AllowBeginList = false;
    } else {
        THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnListItem()
{
    AllowBeginMap = true;
    if (BeforeFirstListItem) {
        BeforeFirstListItem = false;
    } else {
        // Not first item.
        Stream->Write(Config->RecordSeparator);
    }
}

void TDsvNodeConsumer::OnEndList()
{
    Stream->Write(Config->RecordSeparator);
}

void TDsvNodeConsumer::OnBeginMap()
{
    if (AllowBeginMap) {
        AllowBeginList = false;
        AllowBeginMap = false;
        BeforeFirstMapItem = true;
    } else {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnKeyedItem(const TStringBuf& key)
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

void TDsvNodeConsumer::OnEndMap()
{
    YASSERT(!AllowBeginMap);
    YASSERT(!AllowBeginList);
}

void TDsvNodeConsumer::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
}

void TDsvNodeConsumer::OnEndAttributes()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
