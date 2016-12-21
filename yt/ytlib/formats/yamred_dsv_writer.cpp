#include "yamred_dsv_writer.h"

#include "escape.h"

#include <yt/ytlib/table_client/name_table.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NTableClient;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForYamredDsv
    : public TSchemalessWriterForYamrBase
{
public:
    TSchemalessWriterForYamredDsv(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TYamredDsvFormatConfigPtr config = New<TYamredDsvFormatConfig>())
        : TSchemalessWriterForYamrBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount,
            config)
        , Config_(config)
    {
        ConfigureEscapeTables(config, true /* addCarriageReturn */, &KeyEscapeTable_, &ValueEscapeTable_);
        try {
            // We register column names in order to have correct size of NameTable_ in DoWrite method.
            for (const auto& columnName : config->KeyColumnNames) {
                KeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
            }

            for (const auto& columnName : config->SubkeyColumnNames) {
                SubkeyColumnIds_.push_back(nameTable->GetIdOrRegisterName(columnName));
            }
        } catch (const std::exception& ex) {
            auto error = TError("Failed to add columns to name table for YAMRed DSV format")
                << ex;
            RegisterError(error);
        }

        UpdateEscapedColumnNames();
    }

private:
    const TYamredDsvFormatConfigPtr Config_;

    std::vector<TNullable<TStringBuf>> RowValues_;

    std::vector<int> KeyColumnIds_;
    std::vector<int> SubkeyColumnIds_;
    std::vector<Stroka> EscapedColumnNames_;

    // We capture size of name table at the beginnig of #WriteRows
    // and refer to the captured value, since name table may change asynchronously.
    int NameTableSize_ = 0;

    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;


    // ISchemalessFormatWriter implementation
    virtual void DoWrite(const std::vector<TUnversionedRow>& rows) override
    {
        TableIndexWasWritten_ = false;

        auto* stream = GetOutputStream();

        UpdateEscapedColumnNames();
        RowValues_.resize(NameTableSize_);
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
                YCHECK(item->Id < NameTableSize_);
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
            WriteYamrValue();
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    void WriteYamrKey(const std::vector<int>& columnIds)
    {
        auto* stream = GetOutputStream();
        if (Config_->Lenval) {
            ui32 keyLength = CalculateTotalKeyLength(columnIds);
            WritePod(*stream, keyLength);
        }

        bool firstColumn = true;
        for (int id : columnIds) {
            if (!firstColumn) {
                stream->Write(Config_->YamrKeysSeparator);
            } else {
                firstColumn = false;
            }
            if (!RowValues_[id]) {
                THROW_ERROR_EXCEPTION("Key column %Qv is missing",
                    NameTableReader_->GetName(id));
            }
            EscapeAndWrite(*RowValues_[id], stream, ValueEscapeTable_);
            RowValues_[id].Reset();
        }

        if (!Config_->Lenval) {
            stream->Write(Config_->FieldSeparator);
        }
    }

    ui32 CalculateTotalKeyLength(const std::vector<int>& columnIds)
    {
        ui32 sum = 0;
        for (int id : columnIds) {
            if (!RowValues_[id]) {
                THROW_ERROR_EXCEPTION("Key column %Qv is missing",
                    NameTableReader_->GetName(id));
            }
            // Word "value" below is not a mistake, it means the value part of YAMR format.
            sum += CalculateEscapedLength(*RowValues_[id], ValueEscapeTable_);
        }
        if (!columnIds.empty()) {
            sum += columnIds.size() - 1;
        }
        return sum;
    }

    void WriteYamrValue()
    {
        auto* stream = GetOutputStream();

        char keyValueSeparator =
            static_cast<TYamredDsvFormatConfig*>(Config_.Get())->KeyValueSeparator;

        if (Config_->Lenval) {
            ui32 valueLength = CalculateTotalValueLength();
            WritePod(*stream, valueLength);
        }

        bool firstColumn = true;
        for (int id = 0; id < NameTableSize_; ++id) {
            if (RowValues_[id]) {
                if (!firstColumn) {
                    stream->Write(Config_->FieldSeparator);
                } else {
                    firstColumn = false;
                }
                stream->Write(EscapedColumnNames_[id]);
                stream->Write(keyValueSeparator);
                EscapeAndWrite(*RowValues_[id], stream, ValueEscapeTable_);
                RowValues_[id].Reset();
            }
        }

        if (!Config_->Lenval) {
            stream->Write(Config_->RecordSeparator);
        }
    }

    ui32 CalculateTotalValueLength()
    {
        ui32 sum = 0;
        bool firstColumn = true;
        for (int id = 0; id < NameTableSize_; ++id) {
            if (RowValues_[id]) {
                if (!firstColumn) {
                    sum += 1; // The yamr_keys_separator.
                } else {
                    firstColumn = false;
                }
                sum += EscapedColumnNames_[id].length();
                sum += 1; // The key_value_separator.
                sum += CalculateEscapedLength(*RowValues_[id], ValueEscapeTable_);
            }
        }
        return sum;
    }

    void UpdateEscapedColumnNames()
    {
        // We store escaped column names in order to not re-escape them each time we write a column name.
        NameTableSize_ = NameTableReader_->GetSize();
        EscapedColumnNames_.reserve(NameTableSize_);
        for (int columnIndex = EscapedColumnNames_.size(); columnIndex < NameTableSize_; ++columnIndex) {
            EscapedColumnNames_.emplace_back(Escape(NameTableReader_->GetName(columnIndex), KeyEscapeTable_));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    TYamredDsvFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    if (controlAttributesConfig->EnableKeySwitch && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in text YAMRed DSV format");
    }

    if (controlAttributesConfig->EnableRangeIndex && !config->Lenval) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in text YAMRed DSV format");
    }

    return New<TSchemalessWriterForYamredDsv>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForYamredDsv(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForYamredDsv(
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

