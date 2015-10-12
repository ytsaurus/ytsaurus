#include "stdafx.h"
#include "yamr_writer.h"

#include <ytlib/table_client/name_table.h>

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemalessYamrWriter::TSchemalessYamrWriter(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    bool enableKeySwitch,
    int keyColumnCount,
    TYamrFormatConfigPtr config)
    : TSchemalessFormatWriterBase(
        nameTable, 
        std::move(output),
        enableContextSaving, 
        enableKeySwitch,
        keyColumnCount)
    , Config_(config)
    , Table_(
        config->FieldSeparator,
        config->RecordSeparator,
        config->EnableEscaping, // Enable key escaping
        config->EnableEscaping, // Enable value escaping
        config->EscapingSymbol,
        true)
{
    KeyId_ = nameTable->GetIdOrRegisterName(config->Key);
    SubkeyId_ = Config_->HasSubkey ? nameTable->GetIdOrRegisterName(config->Subkey) : -1;
    ValueId_ = nameTable->GetIdOrRegisterName(config->Value);
}

void TSchemalessYamrWriter::EscapeAndWrite(const TStringBuf& value, bool inKey)
{
    auto* stream = GetOutputStream();
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            value,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(value);
    }
}

void TSchemalessYamrWriter::WriteInLenvalMode(const TStringBuf& value)
{
    auto* stream = GetOutputStream();
    WritePod(*stream, static_cast<ui32>(value.size()));
    stream->Write(value);
}
    
void TSchemalessYamrWriter::ValidateColumnType(const TUnversionedValue* value)
{
    if (value->Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Wrong column type %Qlv in YAMR record", value->Type);
    }
}

void TSchemalessYamrWriter::DoWrite(const std::vector<TUnversionedRow>& rows)
{
    auto* stream = GetOutputStream();
    
    for (int i = 0; i < static_cast<int>(rows.size()); i++) {
        if (CheckKeySwitch(rows[i], i + 1 == rows.size() /* isLastRow */)) {
            if (!Config_->Lenval) {
                THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format.");
            }
            WritePod(*stream, static_cast<ui32>(-2));
        }
        
        TNullable<TStringBuf> key;
        TNullable<TStringBuf> subkey;
        TNullable<TStringBuf> value;

        for (const auto* item = rows[i].Begin(); item != rows[i].End(); ++item) {
            if (item->Id == KeyId_) {
                ValidateColumnType(item);
                key = TStringBuf(item->Data.String, item->Length);
            } else if (item->Id == SubkeyId_) {
                if (item->Type != EValueType::Null) {
                    ValidateColumnType(item);
                    subkey = TStringBuf(item->Data.String, item->Length);
                }
            } else if (item->Id == ValueId_) {
                ValidateColumnType(item);
                value = TStringBuf(item->Data.String, item->Length);
            } else {
                // Ignore unknown columns.
                continue;
            }
        }

        if (!key) {
            THROW_ERROR_EXCEPTION("Missing key column %Qv in YAMR record", Config_->Key); 
        }

        if (!subkey) {
            subkey = "";
        }

        if (!value) {
            THROW_ERROR_EXCEPTION("Missing value column %Qv in YAMR record", Config_->Value);
        }
             
        if (!Config_->Lenval) {
            EscapeAndWrite(*key, true);
            stream->Write(Config_->FieldSeparator);
            if (Config_->HasSubkey) {
                EscapeAndWrite(*subkey, true);
                stream->Write(Config_->FieldSeparator);
            }
            EscapeAndWrite(*value, false);
            stream->Write(Config_->RecordSeparator);
        } else {
            WriteInLenvalMode(*key);
            if (Config_->HasSubkey) {
                WriteInLenvalMode(*subkey);
            }
            WriteInLenvalMode(*value);
        }

        TryFlushBuffer();
    }

    TryFlushBuffer();
}

void TSchemalessYamrWriter::WriteTableIndex(int tableIndex)
{
    auto* stream = GetOutputStream();
    
    if (!Config_->EnableTableIndex) {
        // Silently ignore table switches.
        return;
    }

    if (Config_->Lenval) {
        WritePod(*stream, static_cast<ui32>(-1));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    } else {
        stream->Write(ToString(tableIndex));
        stream->Write(Config_->RecordSeparator);
    }
}

void TSchemalessYamrWriter::WriteRangeIndex(i32 rangeIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-3));
    WritePod(*stream, static_cast<ui32>(rangeIndex));
}

void TSchemalessYamrWriter::WriteRowIndex(i64 rowIndex)
{
    auto* stream = GetOutputStream();

    if (!Config_->Lenval) {
         THROW_ERROR_EXCEPTION("Row indices are not supported in text YAMR format");
    }
    WritePod(*stream, static_cast<ui32>(-4));
    WritePod(*stream, static_cast<ui64>(rowIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
