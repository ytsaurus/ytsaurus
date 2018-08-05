#include "public.h"
#include "format.h"
#include "helpers.h"
#include "config.h"

#include "schemaless_writer_adapter.h"
#include "schemaless_web_json_writer.h"

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/yson/format.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ContextBufferCapacity = 1_MB;

////////////////////////////////////////////////////////////////////////////////

class TWrittenSizeAccountedOutputStream
    : public IOutputStream
{
public:
    explicit TWrittenSizeAccountedOutputStream(
        std::unique_ptr<IOutputStream> underlyingStream = nullptr)
    {
        Reset(std::move(underlyingStream));
    }

    void Reset(std::unique_ptr<IOutputStream> underlyingStream)
    {
        UnderlyingStream_ = std::move(underlyingStream);
        WrittenSize_ = 0;
    }

    i64 GetWrittenSize() const
    {
        return WrittenSize_;
    }

protected:
    // For simplicity we do not override DoWriteV and DoWriteC methods here.
    // Overriding DoWrite method is enough for local usage.
    virtual void DoWrite(const void* buf, size_t length) override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Write(buf, length);
            WrittenSize_ += length;
        }
    }

    virtual void DoFlush() override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Flush();
        }
    }

    virtual void DoFinish() override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Finish();
        }
    }

private:
    std::unique_ptr<IOutputStream> UnderlyingStream_;

    i64 WrittenSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TWebJsonColumnFilter
{
public:
    TWebJsonColumnFilter(int maxSelectedColumnCount, TNullable<THashSet<TString>> names)
        : MaxSelectedColumnCount_(maxSelectedColumnCount)
        , Names_(std::move(names))
    { }

    bool Accept(ui16 columnId, TStringBuf columnName)
    {
        if (Names_) {
            return AcceptByNames(columnId, columnName);
        }
        return AcceptByMaxCount(columnId, columnName);
    }

private:
    const int MaxSelectedColumnCount_;
    TNullable<THashSet<TString>> Names_;

    THashSet<ui16> AcceptedColumnIds_;

    bool AcceptByNames(ui16 columnId, TStringBuf columnName)
    {
        return Names_->has(columnName);
    }

    bool AcceptByMaxCount(ui16 columnId, TStringBuf columnName)
    {
        if (AcceptedColumnIds_.size() < MaxSelectedColumnCount_) {
            AcceptedColumnIds_.insert(columnId);
            return true;
        }
        return AcceptedColumnIds_.has(columnId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TWebJsonColumnFilter CreateWebJsonColumnFilter(const TSchemalessWebJsonFormatConfigPtr& webJsonConfig)
{
    TNullable<THashSet<TString>> columnNames;
    if (webJsonConfig->ColumnNames) {
        columnNames.Emplace();
        for (const auto& columnName : *webJsonConfig->ColumnNames) {
            if (!columnNames->insert(columnName).second) {
                THROW_ERROR_EXCEPTION("Duplicate column name %Qv in \"column_names\" parameter of web_json format config",
                    columnName);
            }
        }
    }
    return TWebJsonColumnFilter(webJsonConfig->MaxSelectedColumnCount, std::move(columnNames));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForWebJson
    : public ISchemalessFormatWriter
{
public:
    TSchemalessWriterForWebJson(
        NConcurrency::IAsyncOutputStreamPtr output,
        TSchemalessWebJsonFormatConfigPtr config,
        TNameTablePtr nameTable,
        TWebJsonColumnFilter columnFilter);

    virtual bool Write(TRange<TUnversionedRow> rows) override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual const TNameTablePtr& GetNameTable() const override;
    virtual const TTableSchema& GetSchema() const override;
    virtual TBlob GetContext() const override;
    virtual i64 GetWrittenSize() const override;
    virtual TFuture<void> Close() override;

private:
    const TSchemalessWebJsonFormatConfigPtr Config_;
    const TNameTablePtr NameTable_;
    const TNameTableReader NameTableReader_;

    TWrittenSizeAccountedOutputStream Output_;
    std::unique_ptr<IJsonConsumer> ResponseBuilder_;

    TWebJsonColumnFilter ColumnFilter_;
    THashMap<ui16, TString> AllColumnIdToName_;

    bool IncompleteAllColumnNames_ = false;
    bool IncompleteColumns_ = false;

    TError Error_;

    bool TryRegisterColumn(ui16 columnId, TStringBuf columnName);
    bool SkipSystemColumn(TStringBuf columnName) const;

    void DoFlush(bool force);
    void DoWrite(TRange<TUnversionedRow> rows);
    void DoClose();
};

TSchemalessWriterForWebJson::TSchemalessWriterForWebJson(
    NConcurrency::IAsyncOutputStreamPtr output,
    TSchemalessWebJsonFormatConfigPtr config,
    TNameTablePtr nameTable,
    TWebJsonColumnFilter columnFilter)
    : Config_(std::move(config))
    , NameTable_(std::move(nameTable))
    , NameTableReader_(NameTable_)
    , ColumnFilter_(std::move(columnFilter))
{
    {
        auto bufferedSyncOutput = CreateBufferedSyncAdapter(
            std::move(output),
            ESyncStreamAdapterStrategy::WaitFor,
            ContextBufferCapacity);

        Output_.Reset(std::move(bufferedSyncOutput));
    }

    {
        auto jsonConfig = New<TJsonFormatConfig>();
        jsonConfig->Stringify = true;
        jsonConfig->AnnotateWithTypes = true;

        ResponseBuilder_ = CreateJsonConsumer(
            &Output_,
            NYson::EYsonType::Node,
            std::move(jsonConfig));
    }

    ResponseBuilder_->OnBeginMap();
    ResponseBuilder_->OnKeyedItem("rows");
    ResponseBuilder_->OnBeginList();
}

bool TSchemalessWriterForWebJson::Write(TRange<TUnversionedRow> rows)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWrite(rows);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

TFuture<void> TSchemalessWriterForWebJson::GetReadyEvent()
{
    return MakeFuture(Error_);
}

const TTableSchema& TSchemalessWriterForWebJson::GetSchema() const
{
    Y_UNREACHABLE();
}

const TNameTablePtr& TSchemalessWriterForWebJson::GetNameTable() const
{
    return NameTable_;
}

TBlob TSchemalessWriterForWebJson::GetContext() const
{
    return TBlob();
}

i64 TSchemalessWriterForWebJson::GetWrittenSize() const
{
    return Output_.GetWrittenSize();
}

TFuture<void> TSchemalessWriterForWebJson::Close()
{
    try {
        DoClose();
    } catch (const std::exception& exception) {
        Error_ = TError(exception);
    }

    return GetReadyEvent();
}

bool TSchemalessWriterForWebJson::TryRegisterColumn(ui16 columnId, TStringBuf columnName)
{
    if (SkipSystemColumn(columnName)) {
        return false;
    }

    if (AllColumnIdToName_.size() < Config_->MaxAllColumnNamesCount) {
        AllColumnIdToName_[columnId] = columnName;
    } else if (!AllColumnIdToName_.has(columnId)) {
        IncompleteAllColumnNames_ = true;
    }

    const auto result = ColumnFilter_.Accept(columnId, columnName);
    if (!result) {
        IncompleteColumns_ = true;
    }

    return result;
}

bool TSchemalessWriterForWebJson::SkipSystemColumn(TStringBuf columnName) const
{
    if (!columnName.StartsWith(SystemColumnNamePrefix)) {
        return false;
    }
    return Config_->SkipSystemColumns;
}

void TSchemalessWriterForWebJson::DoFlush(bool force)
{
    ResponseBuilder_->Flush();
    if (force) {
        Output_.Flush();
    }
}

void TSchemalessWriterForWebJson::DoWrite(TRange<TUnversionedRow> rows)
{
    for (int rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
        const auto row = rows[rowIndex];
        if (!row) {
            continue;
        }

        ResponseBuilder_->OnListItem();
        ResponseBuilder_->OnBeginMap();

        for (const auto& value : row) {
            TStringBuf columnName;
            if (!NameTableReader_.TryGetName(value.Id, columnName)) {
                continue;
            }

            if (!TryRegisterColumn(value.Id, columnName)) {
                continue;
            }

            ResponseBuilder_->OnKeyedItem(columnName);

            const TStringBuf valueBuf(value.Data.String, value.Length);
            switch (value.Type) {
                case EValueType::Any:
                    ResponseBuilder_->OnNodeWeightLimited(valueBuf, Config_->FieldWeightLimit);
                    break;
                case EValueType::String:
                    ResponseBuilder_->OnStringScalarWeightLimited(valueBuf, Config_->FieldWeightLimit);
                    break;
                default:
                    WriteYsonValue(ResponseBuilder_.get(), value);
            }
        }

        ResponseBuilder_->OnEndMap();

        DoFlush(false);
    }

    DoFlush(true);
}

void TSchemalessWriterForWebJson::DoClose()
{
    if (Error_.IsOK()) {
        ResponseBuilder_->OnEndList();

        ResponseBuilder_->SetAnnotateWithTypesParameter(false);

        ResponseBuilder_->OnKeyedItem("incomplete_columns");
        ResponseBuilder_->OnBooleanScalar(IncompleteColumns_);

        ResponseBuilder_->OnKeyedItem("incomplete_all_column_names");
        ResponseBuilder_->OnBooleanScalar(IncompleteAllColumnNames_);

        ResponseBuilder_->OnKeyedItem("all_column_names");
        ResponseBuilder_->OnBeginList();

        std::vector<TStringBuf> allColumnNamesSorted;
        allColumnNamesSorted.reserve(AllColumnIdToName_.size());
        for (const auto& columnIdToName : AllColumnIdToName_) {
            allColumnNamesSorted.push_back(columnIdToName.second);
        }
        std::sort(allColumnNamesSorted.begin(), allColumnNamesSorted.end());

        for (const auto columnName : allColumnNamesSorted) {
            ResponseBuilder_->OnListItem();
            ResponseBuilder_->OnStringScalar(columnName);
        }

        ResponseBuilder_->OnEndList();

        ResponseBuilder_->OnEndMap();

        DoFlush(true);
    }
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForWebJson(
    TSchemalessWebJsonFormatConfigPtr config,
    NConcurrency::IAsyncOutputStreamPtr output,
    TNameTablePtr nameTable)
{
    return New<TSchemalessWriterForWebJson>(
        std::move(output),
        std::move(config),
        std::move(nameTable),
        CreateWebJsonColumnFilter(config));
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    NConcurrency::IAsyncOutputStreamPtr stream,
    TNameTablePtr nameTable)
{
    return CreateSchemalessWriterForWebJson(
        ConvertTo<TSchemalessWebJsonFormatConfigPtr>(&attributes),
        std::move(stream),
        std::move(nameTable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
