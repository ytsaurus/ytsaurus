#include "public.h"
#include "format.h"
#include "helpers.h"
#include "config.h"

#include "schemaless_writer_adapter.h"
#include "schemaless_web_json_writer.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/yson/format.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

#include <util/string/escape.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const i64 ContextBufferCapacity = static_cast<i64>(1024) * 1024;
static const TString IncompleteValue = "{\"$incomplete\":\"true\",\"$type\":\"any\"}\n";

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
    virtual TFuture<void> Open() override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual const TNameTablePtr& GetNameTable() const override;
    virtual const TTableSchema& GetSchema() const override;
    virtual TBlob GetContext() const override;
    virtual i64 GetWrittenSize() const override;
    virtual TFuture<void> Close() override;

protected:
    bool IsSystemColumnId(int id) const;
    bool IsTableIndexColumnId(int id) const;
    bool IsRangeIndexColumnId(int id) const;
    bool IsRowIndexColumnId(int id) const;

private:
    const NConcurrency::IAsyncOutputStreamPtr Output_;
    const TSchemalessWebJsonFormatConfigPtr Config_;
    const TNameTablePtr NameTable_;

    std::unique_ptr<NYson::IFlushableYsonConsumer> JsonConsumer_;

    TBlobOutput Buffer_;

    yhash<ui16, int> ColumnIds_;
    std::vector<std::vector<std::pair<TSharedRef, int>>> ResultRows_;

    bool IncompleteRows_ = false;
    bool IncompleteColumns_ = false;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

    TError Error_;

    void DoWrite(const TRange<NTableClient::TUnversionedRow>& rows);
};

TSchemalessWriterForWebJson::TSchemalessWriterForWebJson(
    NConcurrency::IAsyncOutputStreamPtr output,
    TSchemalessWebJsonFormatConfigPtr config,
    TNameTablePtr nameTable)
    : Output_(std::move(output))
    , Config_(std::move(config))
    , NameTable_(std::move(nameTable))
{ 
    Buffer_.Reserve(ContextBufferCapacity);
 
    auto jsonConfig = New<TJsonFormatConfig>();
    jsonConfig->Stringify = true;
    jsonConfig->AnnotateWithTypes = true;
    jsonConfig->BooleanAsString = true;
    jsonConfig->StringLengthLimit = Config_->StringLikeLengthLimit;

    JsonConsumer_ = CreateJsonConsumer(
        &Buffer_,
        NYson::EYsonType::ListFragment,
        jsonConfig);

    try {
        RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to add system columns to name table for a format writer") << ex;
    } 
}

bool TSchemalessWriterForWebJson::Write(const TRange<TUnversionedRow> &rows)
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

TFuture<void> TSchemalessWriterForWebJson::Open()
{
    return VoidFuture;
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
    Y_UNREACHABLE();
}

bool TSchemalessWriterForWebJson::IsSystemColumnId(int id) const
{
    return IsTableIndexColumnId(id) ||
        IsRangeIndexColumnId(id) ||
        IsRowIndexColumnId(id);
}

bool TSchemalessWriterForWebJson::IsTableIndexColumnId(int id) const
{
    return id == TableIndexId_;
}

bool TSchemalessWriterForWebJson::IsRowIndexColumnId(int id) const
{
    return id == RowIndexId_;
}

bool TSchemalessWriterForWebJson::IsRangeIndexColumnId(int id) const
{
    return id == RangeIndexId_;
}

TFuture<void> TSchemalessWriterForWebJson::Close()
{
    int columnCount = static_cast<int>(ColumnIds_.size());

    auto writeSeparatorSymbol = [&] (bool& first, char ch) {
        if (!first) {
            Buffer_.Write(ch);
        } else {
            first = false;
        }
    };

    bool firstValue = true;

    Buffer_.Write('{');

    if (IncompleteRows_) {
        writeSeparatorSymbol(firstValue, ',');
        Buffer_.Write("\"incomplete_rows\":true");
    }

    if (IncompleteColumns_) {
        writeSeparatorSymbol(firstValue, ',');
        Buffer_.Write("\"incomplete_columns\":true");
    }

    std::vector<int> columnsPositions(columnCount);

    for (const auto& columnId : ColumnIds_) {
        columnsPositions[columnId.second] = columnId.first;
    }

    writeSeparatorSymbol(firstValue, ',');
    Buffer_.Write("\"column_names\":");

    Buffer_.Write('[');

    bool firstColumnName = true;
    for (const auto& columnId: columnsPositions) {
        writeSeparatorSymbol(firstColumnName, ',');
        Buffer_.Write("\"" + EscapeC(ToString(NameTable_->GetName(columnId))) + "\"");
    }

    Buffer_.Write(']');

    writeSeparatorSymbol(firstValue, ',');
    Buffer_.Write("\"rows\":");

    Buffer_.Write('[');

    bool firstRow = true;
    for (const auto& row : ResultRows_) {
        int prevColumn = -1;
        bool firstRecord = true;
        int rowLength = static_cast<int>(row.size());
        int count;

        writeSeparatorSymbol(firstRow, ',');

        Buffer_.Write('[');

        for (int i = 0; i < rowLength; ++i) {
            count = row[i].second - prevColumn - 1;
            while (count--) {
                writeSeparatorSymbol(firstRecord, ',');
                Buffer_.Write("null");
            }

            prevColumn = row[i].second;
            writeSeparatorSymbol(firstRecord, ',');
            Buffer_.Write(row[i].first.Begin(), row[i].first.Size() - 1);
        }

        count = columnCount - prevColumn - 1;
        while (count--) {
            writeSeparatorSymbol(firstRecord, ',');
            Buffer_.Write("null");
        }

        Buffer_.Write(']');
    }

    Buffer_.Write(']');

    Buffer_.Write('}');

    TSharedRef result = Buffer_.Flush();
    WaitFor(Output_->Write(result))
        .ThrowOnError();

    Buffer_.Reserve(ContextBufferCapacity);

    return GetReadyEvent();
}


void TSchemalessWriterForWebJson::DoWrite(const TRange<NTableClient::TUnversionedRow>& rows)
{
    IncompleteRows_ = (ResultRows_.size() + rows.Size()) > Config_->RowLimit;

    if (ResultRows_.size() >= Config_->RowLimit) {
        return;
    }

    int rowCount = std::min(static_cast<int>(rows.Size()), Config_->RowLimit - static_cast<int>(ResultRows_.size()));

    int nextUnusedColumnIndex = static_cast<int>(ColumnIds_.size());

    for (int index = 0; index < rowCount && nextUnusedColumnIndex < Config_->ColumnLimit; ++index) {
        auto row = rows[index];

        for (auto* it = row.Begin(); it != row.End() && nextUnusedColumnIndex < Config_->ColumnLimit; ++it) {
            auto& value = *it;
            if (IsSystemColumnId(value.Id)) {
                continue;
            }
            if (ColumnIds_.find(value.Id) == ColumnIds_.end()) {
                ColumnIds_[value.Id] = nextUnusedColumnIndex;
                ++nextUnusedColumnIndex;
            }
        }
    }

    for (int index = 0; index < rowCount; ++index) {
        auto row = rows[index];
        int size = row.GetCount();

        std::vector<std::pair<TSharedRef, int>> currentRow; 

        for (int i = 0; i < size; ++i) {
            auto& value = row[i];
            int columnIndex;

            if (ColumnIds_.find(value.Id) != ColumnIds_.end()) {
                columnIndex = ColumnIds_[value.Id];
            } else {
                if (!IsSystemColumnId(value.Id)) {
                    IncompleteColumns_ = true;
                }
                continue;
            }

            JsonConsumer_->OnListItem();

            switch (value.Type) {
                case EValueType::Int64:
                    JsonConsumer_->OnInt64Scalar(value.Data.Int64);
                    break;
                case EValueType::Uint64:
                    JsonConsumer_->OnUint64Scalar(value.Data.Uint64);
                    break;
                case EValueType::Double:
                    JsonConsumer_->OnDoubleScalar(value.Data.Double);
                    break;
                case EValueType::Boolean:
                    JsonConsumer_->OnBooleanScalar(value.Data.Boolean);
                    break;
                case EValueType::String:
                    JsonConsumer_->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                    break;
                case EValueType::Null:
                    JsonConsumer_->OnEntity();
                    break;
                case EValueType::Any:
                    JsonConsumer_->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                    break;
                default:
                    Y_UNREACHABLE();
            }

            JsonConsumer_->Flush();

            if (value.Type == EValueType::Any && Buffer_.Size() > Config_->StringLikeLengthLimit) {
                Buffer_.Clear();
                Buffer_.Write(IncompleteValue);
            }

            currentRow.push_back(std::make_pair(Buffer_.Flush(), columnIndex));
        }

        std::sort(
            currentRow.begin(),
            currentRow.end(),
            [] (const std::pair<TSharedRef, int>& lhs,
                const std::pair<TSharedRef, int>& rhs) {
                return lhs.second < rhs.second;
            });     

        ResultRows_.emplace_back(std::move(currentRow));
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
    auto config = ConvertTo<TSchemalessWebJsonFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForWebJson(
        config,
        stream,
        nameTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
