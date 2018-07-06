#include "public.h"
#include "format.h"
#include "helpers.h"
#include "config.h"

#include "schemaless_writer_adapter.h"
#include "schemaless_web_json_writer.h"

#include <yt/ytlib/table_client/name_table.h>

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

class TSchemalessWriterForWebJson
    : public ISchemalessFormatWriter
{
public:
    TSchemalessWriterForWebJson(
        NConcurrency::IAsyncOutputStreamPtr output,
        TSchemalessWebJsonFormatConfigPtr config,
        TNameTablePtr nameTable);

    virtual bool Write(const TRange<TUnversionedRow>& rows) override;
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

    THashMap<ui16, TString> AllColumnIdToName_;
    THashSet<ui16> RegisteredColumnIds_;

    bool IncompleteColumns_ = false;

    TError Error_;

    bool TryRegisterColumn(ui16 columnId, TStringBuf columnName);
    bool SkipSystemColumn(TStringBuf columnName) const;

    void DoFlush(bool force);
    void DoWrite(const TRange<TUnversionedRow>& rows);
    void DoClose();
};

TSchemalessWriterForWebJson::TSchemalessWriterForWebJson(
    NConcurrency::IAsyncOutputStreamPtr output,
    TSchemalessWebJsonFormatConfigPtr config,
    TNameTablePtr nameTable)
    : Config_(std::move(config))
    , NameTable_(std::move(nameTable))
    , NameTableReader_(NameTable_)
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

bool TSchemalessWriterForWebJson::Write(const TRange<TUnversionedRow>& rows)
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

    if (AllColumnIdToName_.size() < Config_->AllColumnNamesLimit) {
        AllColumnIdToName_[columnId] = columnName;
    }

    THashSet<ui16>::insert_ctx insertContext;
    const auto iterator = RegisteredColumnIds_.find(columnId, insertContext);
    if (iterator != RegisteredColumnIds_.end()) {
        return true;
    }

    if (RegisteredColumnIds_.size() >= Config_->ColumnLimit) {
        IncompleteColumns_ = true;
        return false;
    }

    RegisteredColumnIds_.insert_direct(columnId, insertContext);
    return true;
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

void TSchemalessWriterForWebJson::DoWrite(const TRange<TUnversionedRow>& rows)
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
        output,
        config,
        nameTable);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    NConcurrency::IAsyncOutputStreamPtr stream,
    TNameTablePtr nameTable)
{
    return CreateSchemalessWriterForWebJson(
        ConvertTo<TSchemalessWebJsonFormatConfigPtr>(&attributes),
        stream,
        nameTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
