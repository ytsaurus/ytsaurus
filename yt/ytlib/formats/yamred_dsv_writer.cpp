#include "yamred_dsv_writer.h"

#include <ytlib/table_client/name_table.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NTableClient;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSchemalessYamredDsvWriter::TSchemalessYamredDsvWriter(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount,
    TYamredDsvFormatConfigPtr config)
    : TSchemalessYamrWriterBase(
        nameTable, 
        std::move(output),
        enableContextSaving, 
        enableKeySwitch,
        keyColumnCount,
        config)
    , Table_(config, true /* addCarriageReturn */) 
{
    // We register column names in order to have correct size of NameTable_ in DoWrite method.
    for (auto columnName : config->KeyColumnNames) {
        KeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
    }
    if (config->HasSubkey) {
        for (auto columnName : config->SubkeyColumnNames) {
            SubkeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
        }
    }
}

void TSchemalessYamredDsvWriter::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) 
{
    auto* stream = GetOutputStream();
    
    RowValues_.resize(NameTable_->GetSize());
    // Invariant: at the beginning of each loop iteration RowValues contains
    // empty TNullable<TStringBuf> in each element.
    for (int i = 0; i < static_cast<int>(rows.size()); i++) { 
        if (CheckKeySwitch(rows[i], i + 1 == rows.size() /* isLastRow */)) {
            if (!Config_->Lenval) {
                THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMRed DSV format.");
            }
            WritePod(*stream, static_cast<ui32>(-2));
        }
        
        for (const auto* item = rows[i].Begin(); item != rows[i].End(); ++item) {
            if (item->Type == EValueType::Null) {
                // Ignore null values.
            } else if (item->Type != EValueType::String) {
                THROW_ERROR_EXCEPTION("YAMRed DSV doesn't support any value type except String and Null");
            }
            YCHECK(item->Id < NameTable_->GetSize());
            RowValues_[item->Id] = TStringBuf(item->Data.String, item->Length);
        }

        WriteYamrKey(KeyColumnIds_);
        if (Config_->HasSubkey) {
            WriteYamrKey(SubkeyColumnIds_);
        }
        WriteYamrValue();
    }
}

void TSchemalessYamredDsvWriter::WriteYamrKey(const std::vector<int>& columnIds) 
{
    char YamrKeysSeparator = 
        static_cast<TYamredDsvFormatConfig*>(Config_.Get())->YamrKeysSeparator;
    auto* stream = GetOutputStream();
    if (Config_->Lenval) {
        ui32 keyLength = CalculateTotalKeyLength(columnIds);
        WritePod(*stream, keyLength);
    }

    bool firstColumn = true;
    for (int id : columnIds) {
        if (!firstColumn) {
            stream->Write(YamrKeysSeparator);
        } else {
            firstColumn = false;
        }
        if (!RowValues_[id]) {
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", NameTable_->GetName(id));
        }
        EscapeAndWrite(*RowValues_[id], false /* inKey */);
        RowValues_[id].Reset();
    }
    
    if (!Config_->Lenval) { 
        stream->Write(Config_->FieldSeparator);
    }    
}

ui32 TSchemalessYamredDsvWriter::CalculateTotalKeyLength(const std::vector<int>& columnIds) 
{
    ui32 sum = 0;
    bool firstColumn = true;
    for (int id : columnIds) {
        if (!RowValues_[id]) {
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", NameTable_->GetName(id));
        }
        if (!firstColumn) {
            sum += 1; // The yamr_keys_separator.
        } else {
            firstColumn = false;
        }
            
        sum += CalculateLength(*RowValues_[id], false /* inKey */);
    }
    return sum;
}

void TSchemalessYamredDsvWriter::WriteYamrValue() 
{
    auto* stream = GetOutputStream();

    char keyValueSeparator = 
        static_cast<TYamredDsvFormatConfig*>(Config_.Get())->KeyValueSeparator;

    if (Config_->Lenval) {
        ui32 valueLength = CalculateTotalValueLength();
        WritePod(*stream, valueLength);
    }

    bool firstColumn = false;
    for (int id = 0; id < NameTable_->GetSize(); id++) {
        if (RowValues_[id]) {
            if (!firstColumn) {
                stream->Write(Config_->FieldSeparator);
            } else {
                firstColumn = false;
            }
            EscapeAndWrite(NameTable_->GetName(id), true /* inKey */);
            stream->Write(keyValueSeparator);
            EscapeAndWrite(*RowValues_[id], false /* inKey */);
        }
    }

    if (!Config_->Lenval) {
        stream->Write(Config_->RecordSeparator);
    }
}

ui32 TSchemalessYamredDsvWriter::CalculateTotalValueLength() 
{
    ui32 sum = 0;
    bool firstColumn = true;
    for (int id = 0; id < NameTable_->GetSize(); id++) {
        if (RowValues_[id]) {
            if (!firstColumn) {
                sum += 1; // The yamr_keys_separator.
            } else {
                firstColumn = false;
            }
            sum += CalculateLength(NameTable_->GetName(id), true);
            sum += 1; // The key_value_separator.
            sum += CalculateLength(*RowValues_[id], false /* inKey */);
        }
    }
    return sum;
}

ui32 TSchemalessYamredDsvWriter::CalculateLength(const TStringBuf& string, bool inKey)
{
    return Config_->EnableEscaping
        ?  CalculateEscapedLength(
            string,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol)
        : string.length();
}


////////////////////////////////////////////////////////////////////////////////

TYamredDsvConsumer::TYamredDsvConsumer(TOutputStream* stream, TYamredDsvFormatConfigPtr config)
    : Stream(stream)
    , Config(config)
    , RowCount(-1)
    , State(EState::None)
    , Table(ConvertTo<TDsvFormatConfigPtr>(config), true /* addCarriageReturn */)
{
    YCHECK(Stream);
    YCHECK(Config);

    for (const auto& name : Config->KeyColumnNames) {
        YCHECK(KeyFields.insert(std::make_pair(name, TColumnValue())).second);
    }
    for (const auto& name : Config->SubkeyColumnNames) {
        YCHECK(SubkeyFields.insert(std::make_pair(name, TColumnValue())).second);
    }
}

TYamredDsvConsumer::~TYamredDsvConsumer()
{ }

void TYamredDsvConsumer::OnInt64Scalar(i64 value)
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Int64 values are not supported by YAMRed DSV");
    }

    YASSERT(State == EState::ExpectAttributeValue);

    // ToDo(psushin): eliminate copy-paste from yamr_writer.cpp
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

        case EControlAttribute::RangeIndex:
            if (!Config->Lenval) {
                THROW_ERROR_EXCEPTION("Range indexes are not supported in text YAMRed DSV");
            }
            WritePod(*Stream, static_cast<ui32>(-3));
            WritePod(*Stream, static_cast<ui32>(value));
            break;

        case EControlAttribute::RowIndex:
            if (!Config->Lenval) {
                 THROW_ERROR_EXCEPTION("Row indexes are not supported in text YAMRed DSV");
            }
            WritePod(*Stream, static_cast<ui32>(-4));
            WritePod(*Stream, static_cast<ui64>(value));
            break;

        default:
            YUNREACHABLE();
    }

    State = EState::ExpectEndAttributes;
}

void TYamredDsvConsumer::OnUint64Scalar(ui64 value)
{
    THROW_ERROR_EXCEPTION("Uint64 values are not supported by YAMRed DSV");
}

void TYamredDsvConsumer::OnDoubleScalar(double value)
{
    THROW_ERROR_EXCEPTION("Double values are not supported by YAMRed DSV");
}

void TYamredDsvConsumer::OnBooleanScalar(bool value)
{
    THROW_ERROR_EXCEPTION("Boolean values are not supported by YAMRed DSV");
}

void TYamredDsvConsumer::OnStringScalar(const TStringBuf& value)
{
    YCHECK(State != EState::ExpectAttributeValue);
    YASSERT(State == EState::ExpectValue);
    State = EState::ExpectColumnName;

    // Compare size before search for optimization.
    // It is not safe in case of repeated keys. Be careful!
    if (KeyCount < KeyFields.size()) {
        auto it = KeyFields.find(ColumnName);
        if (it != KeyFields.end()) {
            it->second.Value = value;
            it->second.RowIndex = RowCount;
            ++KeyCount;
            IncreaseLength(&KeyLength, CalculateLength(value, false));
            return;
        }
    }

    if (SubkeyCount < SubkeyFields.size()) {
        auto it = SubkeyFields.find(ColumnName);
        if (it != SubkeyFields.end()) {
            it->second.Value = value;
            it->second.RowIndex = RowCount;
            ++SubkeyCount;
            IncreaseLength(&SubkeyLength, CalculateLength(value, false));
            return;
        }
    }

    ValueFields.push_back(ColumnName);
    ValueFields.push_back(value);
    IncreaseLength(&ValueLength, CalculateLength(ColumnName, true) + CalculateLength(value, false) + 1);
}

void TYamredDsvConsumer::OnEntity()
{
    if (State == EState::ExpectValue) {
        // Skip null values.
        State = EState::ExpectColumnName;
    } else {
        YASSERT(State == EState::ExpectEntity);
        State = EState::None;
    }
}

void TYamredDsvConsumer::OnBeginList()
{
    YASSERT(State == EState::ExpectValue);
    THROW_ERROR_EXCEPTION("Lists are not supported by YAMRed DSV");
}

void TYamredDsvConsumer::OnListItem()
{
    YASSERT(State == EState::None);
}

void TYamredDsvConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TYamredDsvConsumer::OnBeginMap()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by YAMRed DSV");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectColumnName;

    KeyCount = 0;
    SubkeyCount = 0;

    KeyLength = 0;
    SubkeyLength = 0;
    ValueLength = 0;

    ValueFields.clear();
    ++RowCount;
}

void TYamredDsvConsumer::OnKeyedItem(const TStringBuf& key)
{
    switch (State) {
    case EState::ExpectColumnName:
        ColumnName = key;
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

void TYamredDsvConsumer::OnEndMap()
{
    WriteRow();
    State = EState::None;
}

void TYamredDsvConsumer::OnBeginAttributes()
{
    if (State == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by YAMRed DSV");
    }

    YASSERT(State == EState::None);
    State = EState::ExpectAttributeName;
}

void TYamredDsvConsumer::OnEndAttributes()
{
    YASSERT(State == EState::ExpectEndAttributes);
    State = EState::ExpectEntity;
}

void TYamredDsvConsumer::WriteRow()
{
    if (Config->Lenval) {
        WritePod(*Stream, KeyLength);
        WriteYamrKey(Config->KeyColumnNames, KeyFields, KeyCount);

        if (Config->HasSubkey) {
            WritePod(*Stream, SubkeyLength);
            WriteYamrKey(Config->SubkeyColumnNames, SubkeyFields, SubkeyCount);
        }

        WritePod(*Stream, ValueLength);
        WriteYamrValue();
    } else {
        WriteYamrKey(Config->KeyColumnNames, KeyFields, KeyCount);
        Stream->Write(Config->FieldSeparator);

        if (Config->HasSubkey) {
            WriteYamrKey(Config->SubkeyColumnNames, SubkeyFields, SubkeyCount);
            Stream->Write(Config->FieldSeparator);
        }

        WriteYamrValue();
        Stream->Write(Config->RecordSeparator);
    }
}

void TYamredDsvConsumer::WriteYamrKey(
    const std::vector<Stroka>& columnNames,
    const TDictionary& fieldValues,
    i32 fieldCount)
{
    if (fieldCount < columnNames.size()) {
        for (const auto& column : fieldValues) {
            if (column.second.RowIndex != RowCount) {
                THROW_ERROR_EXCEPTION("Missing column %Qv in YAMRed DSV",
                    column.first);
            }
        }
        YUNREACHABLE();
    }

    auto nameIt = columnNames.begin();
    while (nameIt != columnNames.end()) {
        auto it = fieldValues.find(*nameIt);
        YASSERT(it != fieldValues.end());

        EscapeAndWrite(it->second.Value, false);
        ++nameIt;
        if (nameIt != columnNames.end()) {
            Stream->Write(Config->YamrKeysSeparator);
        }
    }
}

void TYamredDsvConsumer::WriteYamrValue()
{
    YASSERT(ValueFields.size() % 2 == 0);

    auto it = ValueFields.begin();
    while (it !=  ValueFields.end()) {
        // Write key.
        EscapeAndWrite(*it, true);
        ++it;

        Stream->Write(Config->KeyValueSeparator);

        // Write value.
        EscapeAndWrite(*it, false);
        ++it;

        if (it != ValueFields.end()) {
            Stream->Write(Config->FieldSeparator);
        }
    }
}

void TYamredDsvConsumer::EscapeAndWrite(const TStringBuf& string, bool inKey)
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

void TYamredDsvConsumer::IncreaseLength(ui32* length, ui32 delta)
{
    if (*length > 0) {
        *length  += 1;
    }
    *length += delta;
}

ui32 TYamredDsvConsumer::CalculateLength(const TStringBuf& string, bool inKey)
{
    return Config->EnableEscaping
        ?  CalculateEscapedLength(
            string,
            inKey ? Table.KeyStops : Table.ValueStops,
            Table.Escapes,
            Config->EscapingSymbol)
        : string.length();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

