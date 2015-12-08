#include "yamred_dsv_writer.h"

#include <yt/ytlib/table_client/name_table.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NTableClient;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForYamredDsv::TSchemalessWriterForYamredDsv(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
<<<<<<< HEAD
    TControlAttributesConfigPtr controlAttributesConfig,
=======
    bool enableKeySwitch,
>>>>>>> origin/prestable/0.17.4
    int keyColumnCount,
    TYamredDsvFormatConfigPtr config)
    : TSchemalessWriterForYamrBase(
        nameTable, 
        std::move(output),
        enableContextSaving, 
<<<<<<< HEAD
        controlAttributesConfig,
=======
        enableKeySwitch,
>>>>>>> origin/prestable/0.17.4
        keyColumnCount,
        config)
    , Table_(config, true /* addCarriageReturn */) 
{
    // We register column names in order to have correct size of NameTable_ in DoWrite method.
    for (auto columnName : config->KeyColumnNames) {
        KeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
    }
<<<<<<< HEAD

=======
>>>>>>> origin/prestable/0.17.4
    for (auto columnName : config->SubkeyColumnNames) {
        SubkeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
    }
    
    UpdateEscapedColumnNames();
}

void TSchemalessWriterForYamredDsv::UpdateEscapedColumnNames()
{
    // Storing escaped column names in order to not re-escape them each time we write a column name.
<<<<<<< HEAD
    EscapedColumnNames_.reserve(GetNameTable()->GetSize());
    for (int columnIndex = EscapedColumnNames_.size(); columnIndex < GetNameTable()->GetSize(); columnIndex++) {
        EscapedColumnNames_.emplace_back(
                Escape(GetNameTable()->GetName(columnIndex), Table_.KeyStops, Table_.Escapes, Config_->EscapingSymbol));
    }
}

=======
    EscapedColumnNames_.reserve(NameTable_->GetSize());
    for (int columnIndex = EscapedColumnNames_.size(); columnIndex < NameTable_->GetSize(); ++columnIndex) {
        EscapedColumnNames_.emplace_back(
                Escape(NameTable_->GetName(columnIndex), Table_.KeyStops, Table_.Escapes, Config_->EscapingSymbol));
    }
}

int TSchemalessWriterForYamredDsv::GetColumnCount() const
{
    return EscapedColumnNames_.size();
}

>>>>>>> origin/prestable/0.17.4
void TSchemalessWriterForYamredDsv::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) 
{
    auto* stream = GetOutputStream();
    
    UpdateEscapedColumnNames();
<<<<<<< HEAD
    RowValues_.resize(GetNameTable()->GetSize());
    // Invariant: at the beginning of each loop iteration RowValues contains
    // empty TNullable<TStringBuf> in each element.
    for (int i = 0; i < static_cast<int>(rows.size()); i++) {
        auto row = rows[i];
        if (CheckKeySwitch(row, i + 1 == rows.size() /* isLastRow */)) {
            YCHECK (!Config_->Lenval);
            WritePod(*stream, static_cast<ui32>(-2));
        }

        WriteControlAttributes(row);

        for (const auto* item = row.Begin(); item != row.End(); ++item) {
            if (IsSystemColumnId(item->Id) || item->Type == EValueType::Null) {
                // Ignore null values and system columns.
                continue;
            } else if (item->Type != EValueType::String) {
                THROW_ERROR_EXCEPTION("YAMRed DSV doesn't support any value type except String and Null");
            }
            YCHECK(item->Id < GetNameTable()->GetSize());
            RowValues_[item->Id] = TStringBuf(item->Data.String, item->Length);
        }

        WriteYamrKey(KeyColumnIds_);
        if (Config_->HasSubkey) {
            WriteYamrKey(SubkeyColumnIds_);
        } else {
            // Due to YAMRed DSV format logic, when there is no subkey, but still some
            // columns are marked as subkey columns, we should explicitly remove them
            // from the row (i. e. don't print as a rest of values in YAMR value column).
            for (int id : SubkeyColumnIds_)
                RowValues_[id].Reset();
        }
=======
    RowValues_.resize(GetColumnCount());
    // Invariant: at the beginning of each loop iteration RowValues contains
    // empty TNullable<TStringBuf> in each element.
    for (int i = 0; i < static_cast<int>(rows.size()); ++i) { 
        if (CheckKeySwitch(rows[i], i + 1 == rows.size() /* isLastRow */)) {
            if (!Config_->Lenval) {
                THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMRed DSV format.");
            }
            WritePod(*stream, static_cast<ui32>(-2));
        }
        
        for (const auto* item = rows[i].Begin(); item != rows[i].End(); ++item) {
            if (item->Type == EValueType::Null) {
                // Ignore null values.
                continue;
            } else if (item->Type != EValueType::String) {
                THROW_ERROR_EXCEPTION("YAMRed DSV doesn't support any value type except String and Null");
            }
            YCHECK(item->Id < GetColumnCount());
            RowValues_[item->Id] = TStringBuf(item->Data.String, item->Length);
        }

        WriteYamrKey(KeyColumnIds_);
        if (Config_->HasSubkey) {
            WriteYamrKey(SubkeyColumnIds_);
        } else {
            // Due to YAMRed DSV format logic, when there is no subkey, but still some
            // columns are marked as subkey columns, we should explicitly remove them
            // from the row (i. e. don't print as a rest of values in YAMR value column).
            for (int id : SubkeyColumnIds_)
                RowValues_[id].Reset();
        }
>>>>>>> origin/prestable/0.17.4
        WriteYamrValue();
        TryFlushBuffer(false);
    }
    TryFlushBuffer(true);
}

void TSchemalessWriterForYamredDsv::WriteYamrKey(const std::vector<int>& columnIds) 
{
    char yamrKeysSeparator = 
        static_cast<TYamredDsvFormatConfig*>(Config_.Get())->YamrKeysSeparator;
    auto* stream = GetOutputStream();
    if (Config_->Lenval) {
        ui32 keyLength = CalculateTotalKeyLength(columnIds);
        WritePod(*stream, keyLength);
    }

    bool firstColumn = true;
    for (int id : columnIds) {
        if (!firstColumn) {
            stream->Write(yamrKeysSeparator);
        } else {
            firstColumn = false;
        }
        if (!RowValues_[id]) {
<<<<<<< HEAD
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", GetNameTable()->GetName(id));
=======
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", NameTable_->GetName(id));
>>>>>>> origin/prestable/0.17.4
        }
        EscapeAndWrite(*RowValues_[id], Table_.ValueStops, Table_.Escapes);
        RowValues_[id].Reset();
    }
    
    if (!Config_->Lenval) { 
        stream->Write(Config_->FieldSeparator);
    }    
}

<<<<<<< HEAD
ui32 TSchemalessWriterForYamredDsv::CalculateTotalKeyLength(const std::vector<int>& columnIds) 
=======
ui32 TSchemalessWriterForYamredDsv::CalculateTotalKeyLength(const std::vector<int>& columnIds) const 
>>>>>>> origin/prestable/0.17.4
{
    ui32 sum = 0;
    for (int id : columnIds) {
        if (!RowValues_[id]) {
<<<<<<< HEAD
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", GetNameTable()->GetName(id));
=======
            THROW_ERROR_EXCEPTION("Key column %Qv is missing.", NameTable_->GetName(id));
>>>>>>> origin/prestable/0.17.4
        }
            
        sum += CalculateLength(*RowValues_[id], false /* inKey */);
    }
    if (!columnIds.empty()) {
        sum += columnIds.size() - 1;
    }
    return sum;
}

void TSchemalessWriterForYamredDsv::WriteYamrValue() 
{
    auto* stream = GetOutputStream();
<<<<<<< HEAD

    char keyValueSeparator = 
        static_cast<TYamredDsvFormatConfig*>(Config_.Get())->KeyValueSeparator;

=======

    char keyValueSeparator = 
        static_cast<TYamredDsvFormatConfig*>(Config_.Get())->KeyValueSeparator;

>>>>>>> origin/prestable/0.17.4
    if (Config_->Lenval) {
        ui32 valueLength = CalculateTotalValueLength();
        WritePod(*stream, valueLength);
    }

    bool firstColumn = true;
<<<<<<< HEAD
    for (int id = 0; id < GetNameTable()->GetSize(); id++) {
=======
    for (int id = 0; id < GetColumnCount(); ++id) {
>>>>>>> origin/prestable/0.17.4
        if (RowValues_[id]) {
            if (!firstColumn) {
                stream->Write(Config_->FieldSeparator);
            } else {
                firstColumn = false;
            }
            stream->Write(EscapedColumnNames_[id]);
            stream->Write(keyValueSeparator);
            EscapeAndWrite(*RowValues_[id], Table_.ValueStops, Table_.Escapes);
            RowValues_[id].Reset();
        }
    }

    if (!Config_->Lenval) {
        stream->Write(Config_->RecordSeparator);
    }
}

<<<<<<< HEAD
ui32 TSchemalessWriterForYamredDsv::CalculateTotalValueLength() 
{
    ui32 sum = 0;
    bool firstColumn = true;
    for (int id = 0; id < GetNameTable()->GetSize(); id++) {
=======
ui32 TSchemalessWriterForYamredDsv::CalculateTotalValueLength() const 
{
    ui32 sum = 0;
    bool firstColumn = true;
    for (int id = 0; id < GetColumnCount(); ++id) {
>>>>>>> origin/prestable/0.17.4
        if (RowValues_[id]) {
            if (!firstColumn) {
                sum += 1; // The yamr_keys_separator.
            } else {
                firstColumn = false;
            }
            sum += EscapedColumnNames_[id].length();
            sum += 1; // The key_value_separator.
            sum += CalculateLength(*RowValues_[id], false /* inKey */);
        }
    }
    return sum;
}

<<<<<<< HEAD
ui32 TSchemalessWriterForYamredDsv::CalculateLength(const TStringBuf& string, bool inKey)
=======
ui32 TSchemalessWriterForYamredDsv::CalculateLength(const TStringBuf& string, bool inKey) const
>>>>>>> origin/prestable/0.17.4
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

} // namespace NFormats
} // namespace NYT

