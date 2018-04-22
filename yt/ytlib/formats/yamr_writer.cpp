#include "yamr_writer.h"

#include "escape.h"
#include "helpers.h"

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

class TSchemalessWriterForYamr
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamr(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamrFormatConfigPtr config = New<TYamrFormatConfig>())
        : TSchemalessWriterForYamrBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount,
            config)
    {
        ConfigureEscapeTables(
            config,
            config->EnableEscaping /* enableKeyEscaping */,
            config->EnableEscaping /* enableValueEscaping */,
            true /* escapingForWriter */,
            &KeyEscapeTable_,
            &ValueEscapeTable_);

        try {
            KeyId_ = nameTable->GetIdOrRegisterName(config->Key);
            SubkeyId_ = Config_->HasSubkey ? nameTable->GetIdOrRegisterName(config->Subkey) : -1;
            ValueId_ = nameTable->GetIdOrRegisterName(config->Value);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to add columns to name table for YAMR format")
                << ex;
            RegisterError(error);
        }
    }

private:
    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;

    int KeyId_;
    int SubkeyId_;
    int ValueId_;

    // ISchemalessFormatWriter override.
    virtual void DoWrite(const TRange<TUnversionedRow>& rows) override
    {
        TableIndexWasWritten_ = false;

        auto* stream = GetOutputStream();
        // This nasty line is needed to use Config as TYamrFormatConfigPtr
        // without extra serializing/deserializing.
        TYamrFormatConfigPtr config(static_cast<TYamrFormatConfig*>(Config_.Get()));

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; index++) {
            auto row = rows[index];
            if (CheckKeySwitch(row, index + 1 == rowCount /* isLastRow */)) {
                YCHECK(config->Lenval);
                WritePod(*stream, static_cast<ui32>(-2));
            }

            WriteControlAttributes(row);

            TNullable<TStringBuf> key;
            TNullable<TStringBuf> subkey;
            TNullable<TStringBuf> value;

            for (const auto* item = row.Begin(); item != row.End(); ++item) {
                if (item->Id == KeyId_) {
                    ValidateColumnType(item, AsStringBuf("key"));
                    key = TStringBuf(item->Data.String, item->Length);
                } else if (item->Id == SubkeyId_) {
                    if (item->Type != EValueType::Null) {
                        ValidateColumnType(item, AsStringBuf("subkey"));
                        subkey = TStringBuf(item->Data.String, item->Length);
                    }
                } else if (item->Id == ValueId_) {
                    ValidateColumnType(item, AsStringBuf("value"));
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

            if (!config->Lenval) {
                EscapeAndWrite(*key, stream, KeyEscapeTable_);
                stream->Write(config->FieldSeparator);
                if (config->HasSubkey) {
                    EscapeAndWrite(*subkey, stream, KeyEscapeTable_);
                    stream->Write(config->FieldSeparator);
                }
                EscapeAndWrite(*value, stream, ValueEscapeTable_);
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

    void ValidateColumnType(const TUnversionedValue* value, const TStringBuf& columnName)
    {
        if (value->Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Wrong type %Qlv of column %Qv in YAMR record",
                value->Type,
                columnName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamr(
    TYamrFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    if (controlAttributesConfig->EnableKeySwitch && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMR format");
    }

    if (controlAttributesConfig->EnableRangeIndex && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMR format");
    }

    return New<TSchemalessWriterForYamr>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamr(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TYamrFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForYamr(
        config,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
