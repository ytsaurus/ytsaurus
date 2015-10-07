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
    KeyId_ = nameTable->GetId(config->Key);
    SubKeyId_ = (Config_->HasSubkey) ? nameTable->GetId(config->Subkey) : -1;
    ValueId_ = nameTable->GetId(config->Value);
    
    if (enableKeySwitch && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format");
    }
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

void TSchemalessYamrWriter::DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows)
{
    auto* stream = GetOutputStream();

    for (int i = 0; i < static_cast<int>(rows.size()); i++) {
        if (EnableKeySwitch_) {
            if (CheckKeySwitch(rows[i], i + 1 == rows.size() /* isLastRow */)) {
                // It's guaranteed that we are in lenval mode.
                WritePod(*stream, static_cast<ui32>(-2));
            }
        }
        
        TNullable<TStringBuf> key, subkey, value;

        for (const auto* item = rows[i].Begin(); item != rows[i].End(); ++item) {
            if (item->Id == KeyId_) {
                YASSERT(item->Type == EValueType::String);
                key = item->Data.String;
            } else if (item->Id == SubKeyId_) {
                if (item->Type != EValueType::Null) {
                    YASSERT(item->Type == EValueType::String);
                    subkey = item->Data.String;
                }
            } else if (item->Id == ValueId_) {
                YASSERT(item->Type == EValueType::String);
                value = item->Data.String;
            } else {
                THROW_ERROR_EXCEPTION("An item is not a key, a subkey nor a value in YAMR record");
            }
        }

        if (!key) {
            THROW_ERROR_EXCEPTION("Missing key column %Qv in YAMR record", Config_->Key); 
        }

        if (!subkey)
            subkey = "";

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
