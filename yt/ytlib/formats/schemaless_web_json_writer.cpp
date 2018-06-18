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
    std::unique_ptr<IWeightLimitAwareYsonConsumer> JsonConsumer_;

    THashSet<ui16> RegisteredColumnIds_;

    bool IncompleteColumns_ = false;

    TError Error_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

    bool IsSystemColumnId(int id) const;

    bool TryRegisterColumn(ui16 columnId);

    void DoFlush();
    void DoWrite(const TRange<TUnversionedRow>& rows);
    void DoClose();
};

bool TSchemalessWriterForWebJson::TryRegisterColumn(ui16 columnId)
{
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
        jsonConfig->StringLengthLimit = Config_->FieldWeightLimit;

        JsonConsumer_ = CreateJsonConsumer(
            &Output_,
            NYson::EYsonType::Node,
            std::move(jsonConfig));
    }

    JsonConsumer_->OnBeginMap();
    JsonConsumer_->OnKeyedItem("rows");
    JsonConsumer_->OnBeginList();

    try {
        RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to add system columns to name table for a format writer") << ex;
    }
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
    Y_UNREACHABLE();
}

TBlob TSchemalessWriterForWebJson::GetContext() const
{
    Y_UNREACHABLE();
}

i64 TSchemalessWriterForWebJson::GetWrittenSize() const
{
    return Output_.GetWrittenSize();
}

bool TSchemalessWriterForWebJson::IsSystemColumnId(int id) const
{
    return id == TableIndexId_ ||
        id == RowIndexId_ ||
        id == RangeIndexId_;
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

void TSchemalessWriterForWebJson::DoFlush()
{
    JsonConsumer_->Flush();
    Output_.Flush();
}

void TSchemalessWriterForWebJson::DoWrite(const TRange<TUnversionedRow>& rows)
{
    for (int rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
        const auto row = rows[rowIndex];
        if (!row) {
            continue;
        }

        JsonConsumer_->OnListItem();
        JsonConsumer_->OnBeginMap();

        for (const auto& value : row) {
            if (IsSystemColumnId(value.Id)) {
                continue;
            }

            if (!TryRegisterColumn(value.Id)) {
                continue;
            }

            JsonConsumer_->OnKeyedItem(NameTableReader_.GetName(value.Id));

            if (value.Type == EValueType::Any) {
                JsonConsumer_->OnNodeWeightLimited(
                    TStringBuf(value.Data.String, value.Length),
                    Config_->FieldWeightLimit);
            } else {
                WriteYsonValue(JsonConsumer_.get(), value);
            }
        }

        JsonConsumer_->OnEndMap();
    }

    DoFlush();
}

void TSchemalessWriterForWebJson::DoClose()
{
    JsonConsumer_->OnEndList();

    JsonConsumer_->OnKeyedItem("incomplete_columns");
    JsonConsumer_->OnBooleanScalar(IncompleteColumns_);

    JsonConsumer_->OnEndMap();

    DoFlush();
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
    auto config = ConvertTo<TSchemalessWebJsonFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForWebJson(
        config,
        stream,
        nameTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
