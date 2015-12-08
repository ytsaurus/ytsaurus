#include "yamr_writer.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemalessWriterForYamr::TSchemalessWriterForYamr(
    TNameTablePtr nameTable, 
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
<<<<<<< HEAD
    TControlAttributesConfigPtr controlAttributesConfig,
=======
    bool enableKeySwitch,
>>>>>>> origin/prestable/0.17.4
    int keyColumnCount,
    TYamrFormatConfigPtr config)
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
<<<<<<< HEAD

=======
    
>>>>>>> origin/prestable/0.17.4
void TSchemalessWriterForYamr::ValidateColumnType(const TUnversionedValue* value)
{
    if (value->Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Wrong column type %Qlv in YAMR record", value->Type);
    }
}

void TSchemalessWriterForYamr::DoWrite(const std::vector<TUnversionedRow>& rows)
{
    auto* stream = GetOutputStream();
    // This nasty line is needed to use Config as TYamrFormatConfigPtr
    // without extra serializing/deserializing.
    TYamrFormatConfigPtr config(static_cast<TYamrFormatConfig*>(Config_.Get()));

    for (int i = 0; i < static_cast<int>(rows.size()); i++) {
<<<<<<< HEAD
        auto row = rows[i];
        if (CheckKeySwitch(row, i + 1 == rows.size() /* isLastRow */)) {
            YCHECK(config->Lenval);
            WritePod(*stream, static_cast<ui32>(-2));
        }

        WriteControlAttributes(row);
=======
        if (CheckKeySwitch(rows[i], i + 1 == rows.size() /* isLastRow */)) {
            if (!config->Lenval) {
                THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format.");
            }
            WritePod(*stream, static_cast<ui32>(-2));
        }
>>>>>>> origin/prestable/0.17.4
        
        TNullable<TStringBuf> key;
        TNullable<TStringBuf> subkey;
        TNullable<TStringBuf> value;

<<<<<<< HEAD
        for (const auto* item = row.Begin(); item != row.End(); ++item) {
=======
        for (const auto* item = rows[i].Begin(); item != rows[i].End(); ++item) {
>>>>>>> origin/prestable/0.17.4
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
            THROW_ERROR_EXCEPTION("Missing key column %Qv in YAMR record", config->Key); 
        }

        if (!subkey) {
            subkey = "";
        }

        if (!value) {
            THROW_ERROR_EXCEPTION("Missing value column %Qv in YAMR record", config->Value);
        }
<<<<<<< HEAD

=======
             
>>>>>>> origin/prestable/0.17.4
        if (!config->Lenval) {
            EscapeAndWrite(*key, Table_.KeyStops, Table_.Escapes);
            stream->Write(config->FieldSeparator);
            if (config->HasSubkey) {
                EscapeAndWrite(*subkey, Table_.KeyStops, Table_.Escapes);
                stream->Write(config->FieldSeparator);
            }
            EscapeAndWrite(*value, Table_.ValueStops, Table_.Escapes);
            stream->Write(config->RecordSeparator);
        } else {
            WriteInLenvalMode(*key);
            if (config->HasSubkey) {
                WriteInLenvalMode(*subkey);
            }
            WriteInLenvalMode(*value);
        }

        TryFlushBuffer(false);
    }

    TryFlushBuffer(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
