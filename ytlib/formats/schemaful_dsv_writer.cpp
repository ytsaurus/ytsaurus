#include "schemaful_dsv_writer.h"

#include "escape.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/string.h>

#include <yt/core/yson/format.h>

#include <yt/core/concurrency/async_stream.h>

#include <limits>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// This class contains methods common for TSchemafulWriterForSchemafulDsv and TSchemalessWriterForSchemafulDsv.
class TSchemafulDsvWriterBase
{
protected:
    TBlobOutput* BlobOutput_;

    TSchemafulDsvFormatConfigPtr Config_;

    // This array indicates on which position should each
    // column stay in the resulting row.
    std::vector<int> IdToIndexInRow_;

    // This array contains TUnversionedValue's reordered
    // according to the desired order.
    std::vector<const TUnversionedValue*> CurrentRowValues_;

    TEscapeTable EscapeTable_;

    TSchemafulDsvWriterBase(TSchemafulDsvFormatConfigPtr config, std::vector<int> idToIndexInRow)
        : Config_(config)
        , IdToIndexInRow_(idToIndexInRow)
    {
        ConfigureEscapeTable(Config_, &EscapeTable_);
        if (!IdToIndexInRow_.empty()) {
            CurrentRowValues_.resize(
                *std::max_element(IdToIndexInRow_.begin(), IdToIndexInRow_.end()) + 1);
        }
        YCHECK(Config_->Columns);
    }

    void WriteColumnNamesHeader()
    {
        if (Config_->EnableColumnNamesHeader && *Config_->EnableColumnNamesHeader) {
            auto columns = Config_->Columns.Get();
            for (size_t index = 0; index < columns.size(); ++index) {
                WriteRaw(columns[index]);
                WriteRaw((index + 1 == columns.size()) ? Config_->RecordSeparator : Config_->FieldSeparator);
            }
        }
    }

    void WriteRaw(const TStringBuf& str)
    {
        BlobOutput_->Write(str.begin(), str.length());
    }

    void WriteRaw(char ch)
    {
        BlobOutput_->Write(ch);
    }

    int FindMissingValueIndex() const
    {
        for (int valueIndex = 0; valueIndex < static_cast<int>(CurrentRowValues_.size()); ++valueIndex) {
            const auto* value = CurrentRowValues_[valueIndex];
            if (!value || value->Type == EValueType::Null) {
                return valueIndex;
            }
        }
        return -1;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForSchemafulDsv
    : public TSchemalessFormatWriterBase
    , public TSchemafulDsvWriterBase
{
public:
    TSchemalessWriterForSchemafulDsv(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> idToIndexInRow)
        : TSchemalessFormatWriterBase(
            nameTable,
            std::move(output),
            enableContextSaving,
            controlAttributesConfig,
            0 /* keyColumnCount */)
        , TSchemafulDsvWriterBase(
            config,
            idToIndexInRow)
        , TableIndexColumnId_(Config_->EnableTableIndex && controlAttributesConfig->EnableTableIndex
            ? nameTable->GetId(TableIndexColumnName)
            : -1)
    {
        BlobOutput_ = GetOutputStream();
        WriteColumnNamesHeader();
    }

private:
    const int TableIndexColumnId_;

    // ISchemalessFormatWriter overrides.
    virtual void DoWrite(const TRange<TUnversionedRow>& rows) override
    {
        for (const auto& row : rows) {
            CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
            for (auto item = row.Begin(); item != row.End(); ++item) {
                if (item->Id < IdToIndexInRow_.size() && IdToIndexInRow_[item->Id] != -1) {
                    CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
                }
            }

            if (Config_->EnableTableIndex && ControlAttributesConfig_->EnableTableIndex &&
                !CurrentRowValues_[IdToIndexInRow_[TableIndexColumnId_]])
            {
                THROW_ERROR_EXCEPTION("Table index column is missing");
            }

            int missingValueIndex = FindMissingValueIndex();
            if (missingValueIndex != -1) {
                if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                    continue;
                } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) {
                    THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
                }
            }

            bool firstValue = true;
            for (const auto* item : CurrentRowValues_) {
                if (!firstValue) {
                    WriteRaw(Config_->FieldSeparator);
                } else {
                    firstValue = false;
                }
                if (!item || item->Type == EValueType::Null) {
                    // If we got here, MissingValueMode is PrintSentinel.
                    WriteRaw(Config_->MissingValueSentinel);
                } else {
                    WriteUnversionedValue(*item, BlobOutput_, EscapeTable_);
                }
            }
            WriteRaw(Config_->RecordSeparator);
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriterForSchemafulDsv
    : public ISchemafulWriter
    , public TSchemafulDsvWriterBase
{
public:
    TSchemafulWriterForSchemafulDsv(
        IAsyncOutputStreamPtr stream,
        TSchemafulDsvFormatConfigPtr config,
        std::vector<int> IdToIndexInRow)
        : TSchemafulDsvWriterBase(
            config,
            IdToIndexInRow)
        , Output_(CreateSyncAdapter(stream))
    {
        BlobOutput_ = &UnderlyingBlobOutput_;
        WriteColumnNamesHeader();
    }

    virtual TFuture<void> Close() override
    {
        DoFlushBuffer();
        return VoidFuture;
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        for (const auto& row : rows) {
            if (!row) {
                THROW_ERROR_EXCEPTION("Empty rows are not supported by schemaful dsv writer");
            }

            CurrentRowValues_.assign(CurrentRowValues_.size(), nullptr);
            for (auto item = row.Begin(); item != row.End(); ++item) {
                Y_ASSERT(item->Id >= 0 && item->Id < IdToIndexInRow_.size());
                if (IdToIndexInRow_[item->Id] != -1) {
                    CurrentRowValues_[IdToIndexInRow_[item->Id]] = item;
                }
            }

            int missingValueIndex = FindMissingValueIndex();
            if (missingValueIndex != -1) {
                if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::SkipRow) {
                    continue;
                } else if (Config_->MissingValueMode == EMissingSchemafulDsvValueMode::Fail) {
                    THROW_ERROR_EXCEPTION("Column %Qv is in schema but missing", (*Config_->Columns)[missingValueIndex]);
                }
            }

            bool firstValue = true;
            for (const auto* item : CurrentRowValues_) {
                if (!firstValue) {
                    WriteRaw(Config_->FieldSeparator);
                } else {
                    firstValue = false;
                }
                if (!item || item->Type == EValueType::Null) {
                    // If we got here, MissingValueMode is PrintSentinel.
                    WriteRaw(Config_->MissingValueSentinel);
                } else {
                    WriteUnversionedValue(*item, BlobOutput_, EscapeTable_);
                }
            }
            WriteRaw(Config_->RecordSeparator);
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return Result_;
    }

private:
    std::unique_ptr<TOutputStream> Output_;

    // TODO(max42): Eliminate copy-paste from schemaless_writer_adapter.cpp.
    void TryFlushBuffer(bool force)
    {
        if (force || UnderlyingBlobOutput_.Size() >= UnderlyingBlobOutput_.Blob().Capacity() / 2) {
            DoFlushBuffer();
        }
    }

    void DoFlushBuffer()
    {
        if (UnderlyingBlobOutput_.Size() == 0) {
            return;
        }

        const auto& buffer = UnderlyingBlobOutput_.Blob();
        Output_->Write(buffer.Begin(), buffer.Size());

        UnderlyingBlobOutput_.Clear();
    }

    TFuture<void> Result_;

    TBlobOutput UnderlyingBlobOutput_;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateDuplicateColumns(const std::vector<TString>& columns)
{
    yhash_set<TString> names;
    for (const auto& name : columns) {
        if (!names.insert(name).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in schemaful DSV config",
                name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */)
{
    if (controlAttributesConfig->EnableKeySwitch) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in schemaful DSV format");
    }

    if (controlAttributesConfig->EnableRangeIndex) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in schemaful DSV format");
    }

    if (controlAttributesConfig->EnableRowIndex) {
        THROW_ERROR_EXCEPTION("Row indices are not supported in schemaful DSV format");
    }

    if (!config->Columns) {
        THROW_ERROR_EXCEPTION("Config must contain columns for schemaful DSV schemaless writer");
    }

    std::vector<int> idToIndexInRow;
    auto columns = config->Columns.Get();

    if (config->EnableTableIndex && controlAttributesConfig->EnableTableIndex) {
        columns.insert(columns.begin(), TableIndexColumnName);
    }

    ValidateDuplicateColumns(columns);

    try {
        for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
            nameTable->GetIdOrRegisterName(columns[columnIndex]);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to add columns to name table for schemaful DSV format")
            << ex;
    }

    idToIndexInRow.resize(nameTable->GetSize(), -1);
    for (int columnIndex = 0; columnIndex < static_cast<int>(columns.size()); ++columnIndex) {
        idToIndexInRow[nameTable->GetId(columns[columnIndex])] = columnIndex;
    }

    return New<TSchemalessWriterForSchemafulDsv>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        config,
        idToIndexInRow);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForSchemafulDsv(
        config,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    const TTableSchema& schema,
    IAsyncOutputStreamPtr stream)
{
    std::vector<int> idToIndexInRow(schema.Columns().size(), -1);
    if (config->Columns) {
        ValidateDuplicateColumns(*config->Columns);
        for (int columnIndex = 0; columnIndex < static_cast<int>(config->Columns->size()); ++columnIndex) {
            idToIndexInRow[schema.GetColumnIndexOrThrow((*config->Columns)[columnIndex])] = columnIndex;
        }
    } else {
        for (int id = 0; id < static_cast<int>(schema.Columns().size()); ++id) {
            idToIndexInRow[id] = id;
        }
    }

    return New<TSchemafulWriterForSchemafulDsv>(
        stream,
        config,
        idToIndexInRow);
}

ISchemafulWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const IAttributeDictionary& attributes,
    const TTableSchema& schema,
    IAsyncOutputStreamPtr stream)
{
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return CreateSchemafulWriterForSchemafulDsv(config, schema, stream);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
