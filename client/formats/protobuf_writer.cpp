#include "protobuf_writer.h"

#include "lenval_control_constants.h"
#include "protobuf.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/varint.h>

#include <yt/core/yson/pull_parser.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>
#include <util/stream/mem.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

// This class is responsible for writing "other columns" field in protobuf format.
//
// |OnBeginRow|, |OnValue|, |OnEndRow|, |GetProtobufSize| and |WriteProtoField|
// methods can be called even if there is no "other columns" field in current table descriptor,
// in which case they will be effectively no-op.
class TOtherColumnsWriter
{
public:
    TOtherColumnsWriter(
        const TNameTablePtr& nameTable,
        const TProtobufFormatDescriptionPtr& description)
        : NameTableReader_(nameTable)
        , Writer_(
            &OutputStream_,
            EYsonFormat::Binary,
            EYsonType::Node,
            /* enableRaw */ true)
    {
        TableIndexToOtherColumnsField_.resize(description->GetTableCount());
        for (size_t tableIndex = 0; tableIndex < description->GetTableCount(); ++tableIndex) {
            const auto& tableDescription = description->GetTableDescription(tableIndex);
            for (const auto& [name, fieldDescription] : tableDescription.Columns) {
                if (fieldDescription.Type == EProtobufType::OtherColumns) {
                    TableIndexToOtherColumnsField_[tableIndex] = &fieldDescription;
                    break;
                }
            }
        }

        try {
            RowIndexColumnId_ = nameTable->GetIdOrRegisterName(RowIndexColumnName);
            RangeIndexColumnId_ = nameTable->GetIdOrRegisterName(RangeIndexColumnName);
            TableIndexColumnId_ = nameTable->GetIdOrRegisterName(TableIndexColumnName);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to add system columns to name table for protobuf writer")
                << ex;
        }
    }

    void SetTableIndex(i64 tableIndex)
    {
        YT_VERIFY(!InsideRow_);
        FieldDescription_ = TableIndexToOtherColumnsField_[tableIndex];
    }

    bool IsEnabled() const
    {
        return FieldDescription_ != nullptr;
    }

    void OnBeginRow()
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(!InsideRow_);
        OutputStream_.Clear();
        Writer_.OnBeginMap();
        InsideRow_ = true;
    }

    void OnValue(TUnversionedValue value)
    {
        if (!IsEnabled()) {
            return;
        }

        if (IsSystemColumnId(value.Id)) {
            return;
        }

        YT_VERIFY(InsideRow_);
        Writer_.OnKeyedItem(NameTableReader_.GetName(value.Id));
        Serialize(value, &Writer_, /* anyAsRaw */ true);
    }

    void OnEndRow()
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(InsideRow_);
        Writer_.OnEndMap();
        InsideRow_ = false;
    }

    i64 GetProtobufSize() const
    {
        if (!IsEnabled()) {
            return 0;
        }

        YT_VERIFY(!InsideRow_);
        auto length = GetYsonString().size();
        return FieldDescription_->TagSize + WireFormatLite::UInt32Size(length) + length;
    }

    void WriteProtoField(IOutputStream* stream) const
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(!InsideRow_);
        WriteVarUint32(stream, FieldDescription_->WireTag);
        auto buffer = GetYsonString();
        WriteVarUint32(stream, buffer.size());
        stream->Write(buffer.begin(), buffer.size());
    }

private:
    TStringBuf GetYsonString() const
    {
        return OutputStream_.Blob().ToStringBuf();
    }

    bool IsSystemColumnId(int id) const
    {
        return
            TableIndexColumnId_ == id ||
            RangeIndexColumnId_ == id ||
            RowIndexColumnId_ == id;
    }

private:
    const TNameTableReader NameTableReader_;

    std::vector<const TProtobufFieldDescription*> TableIndexToOtherColumnsField_;
    const TProtobufFieldDescription* FieldDescription_ = nullptr;

    TBlobOutput OutputStream_;
    TYsonWriter Writer_;

    bool InsideRow_ = false;

    int RowIndexColumnId_ = -1;
    int RangeIndexColumnId_ = -1;
    int TableIndexColumnId_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TValueGetter>
void WriteProtobufField(
    IOutputStream* stream,
    const TProtobufFieldDescription& fieldDescription,
    const TValueGetter& getter)
{
    switch (fieldDescription.Type) {
        case EProtobufType::String:
        case EProtobufType::Bytes:
        case EProtobufType::Message: {
            auto stringBuf = getter.GetString();
            WriteVarUint32(stream, stringBuf.size());
            stream->Write(stringBuf.data(), stringBuf.size());
            return;
        }
        case EProtobufType::Uint64:
            WriteVarUint64(stream, getter.GetUint64());
            return;
        case EProtobufType::Uint32:
            WriteVarUint32(stream, getter.GetUint64());
            return;
        case EProtobufType::Int64:
            WriteVarUint64(stream, getter.GetInt64()); // no zigzag
            return;
        case EProtobufType::Int32:
            WriteVarUint64(stream, getter.GetInt64()); // no zigzag
            return;
        case EProtobufType::Sint64:
            WriteVarInt64(stream, getter.GetInt64()); // zigzag
            return;
        case EProtobufType::Sint32:
            WriteVarInt32(stream, getter.GetInt64()); // zigzag
            return;
        case EProtobufType::Fixed64:
            WritePod(*stream, getter.GetUint64());
            return;
        case EProtobufType::Fixed32:
            WritePod(*stream, static_cast<ui32>(getter.GetUint64()));
            return;
        case EProtobufType::Sfixed64:
            WritePod(*stream, getter.GetInt64());
            return;
        case EProtobufType::Sfixed32:
            WritePod(*stream, static_cast<i32>(getter.GetInt64()));
            return;
        case EProtobufType::Double:
            WritePod(*stream, getter.GetDouble());
            return;
        case EProtobufType::Float:
            WritePod(*stream, static_cast<float>(getter.GetDouble()));
            return;
        case EProtobufType::Bool:
            WritePod(*stream, static_cast<ui8>(getter.GetBoolean()));
            return;
        case EProtobufType::EnumInt:
        case EProtobufType::EnumString: {
            i32 enumValue;
            bool inRange = true;
            auto getEnumerationName = [&] () {
                return fieldDescription.EnumerationDescription
                    ? fieldDescription.EnumerationDescription->GetEnumerationName()
                    : "<unknown>";
            };
            switch (getter.GetType()) {
                case EValueType::Uint64:
                    inRange = TryIntegralCast<i32>(getter.GetUint64(), &enumValue);
                    break;
                case EValueType::Int64:
                    inRange = TryIntegralCast<i32>(getter.GetInt64(), &enumValue);
                    break;
                case EValueType::String:
                    if (Y_UNLIKELY(!fieldDescription.EnumerationDescription)) {
                        THROW_ERROR_EXCEPTION("Enumeration description not found for field %Qv",
                            fieldDescription.Name);
                    }
                    enumValue = fieldDescription.EnumerationDescription->GetValue(getter.GetString());
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Cannot parse protobuf enumeration %Qv from value of type %Qlv",
                        getEnumerationName(),
                        getter.GetType());
            }
            if (Y_UNLIKELY(!inRange)) {
                THROW_ERROR_EXCEPTION("Value out of range for protobuf enumeration %Qv",
                    getEnumerationName());
            }
            WriteVarUint64(stream, enumValue); // No zigzag int32.
            return;
        }

        case EProtobufType::Any:
        case EProtobufType::OtherColumns:
        case EProtobufType::StructuredMessage:
            THROW_ERROR_EXCEPTION("Wrong protobuf type %Qlv",
                fieldDescription.Type);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateYsonCursorType(const TYsonPullParserCursor* cursor, EYsonItemType expected)
{
    auto actual = cursor->GetCurrent().GetType();
    if (Y_UNLIKELY(actual != expected)) {
        THROW_ERROR_EXCEPTION("Protobuf writing error: bad YSON item, expected %Qlv, actual %Qlv",
            expected,
            actual);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYsonValueGetter
{
public:
    explicit TYsonValueGetter(TYsonPullParserCursor* cursor)
        : Cursor_(cursor)
    { }

    EValueType GetType() const
    {
        switch (Cursor_->GetCurrent().GetType()) {
            case EYsonItemType::EntityValue:
                return EValueType::Null;
            case EYsonItemType::BooleanValue:
                return EValueType::Boolean;
            case EYsonItemType::Int64Value:
                return EValueType::Int64;
            case EYsonItemType::Uint64Value:
                return EValueType::Uint64;
            case EYsonItemType::DoubleValue:
                return EValueType::Double;
            case EYsonItemType::StringValue:
                return EValueType::String;
            default:
                THROW_ERROR_EXCEPTION("EYsonItemType %Qlv cannot be converted to EValueType",
                    Cursor_->GetCurrent().GetType());
        }
    }

    i64 GetInt64() const
    {
        ValidateYsonCursorType(Cursor_, EYsonItemType::Int64Value);
        return Cursor_->GetCurrent().UncheckedAsInt64();
    }

    ui64 GetUint64() const
    {
        ValidateYsonCursorType(Cursor_, EYsonItemType::Uint64Value);
        return Cursor_->GetCurrent().UncheckedAsUint64();
    }

    TStringBuf GetString() const
    {
        ValidateYsonCursorType(Cursor_, EYsonItemType::StringValue);
        return Cursor_->GetCurrent().UncheckedAsString();
    }

    bool GetBoolean() const
    {
        ValidateYsonCursorType(Cursor_, EYsonItemType::BooleanValue);
        return Cursor_->GetCurrent().UncheckedAsBoolean();
    }

    double GetDouble() const
    {
        ValidateYsonCursorType(Cursor_, EYsonItemType::DoubleValue);
        return Cursor_->GetCurrent().UncheckedAsDouble();
    }

private:
    const TYsonPullParserCursor* const Cursor_;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateUnversionedValueType(const TUnversionedValue& value, EValueType type)
{
    if (Y_UNLIKELY(value.Type != type)) {
        THROW_ERROR_EXCEPTION("Invalid protobuf storage type: expected %Qlv, got %Qlv",
            type,
            value.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedValueGetter
{
public:
    explicit TUnversionedValueGetter(TUnversionedValue value)
        : Value_(value)
    { }

    EValueType GetType() const
    {
        return Value_.Type;
    }

    i64 GetInt64() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Int64);
        return Value_.Data.Int64;
    }

    ui64 GetUint64() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Uint64);
        return Value_.Data.Uint64;
    }

    TStringBuf GetString() const
    {
        ValidateUnversionedValueType(Value_, EValueType::String);
        return {Value_.Data.String, Value_.Length};
    }

    bool GetBoolean() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Boolean);
        return Value_.Data.Boolean;
    }

    double GetDouble() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Double);
        return Value_.Data.Double;
    }

private:
    const TUnversionedValue Value_;
};

////////////////////////////////////////////////////////////////////////////////

// Implementation details:
// Actual data chunks are accumulated in |Buffer_|.
// The |Result_| vector stores sequence of "nodes" to produce the final result (in |WriteResult()|).
//   * "Data node" describes a continuous chunk in |Buffer_|.
//   * "Header node" describes a pair (wire tag, size) that preceeds some
//     protobuf types (strings and messages). Header nodes are necessary when we do not know
//     the size of the following message.
class TWriterImpl
{
public:
    TWriterImpl(
        const TNameTablePtr& nameTable,
        const TProtobufFormatDescriptionPtr& description)
        : OtherColumnsWriter_(nameTable, description)
        , BufferOutput_(Buffer_)
    { }

    void SetTableIndex(i64 tableIndex)
    {
        OtherColumnsWriter_.SetTableIndex(tableIndex);
    }

    void OnBeginRow()
    {
        TotalSize_ = 0;
        Result_.clear();
        Buffer_.Clear();
        OtherColumnsWriter_.OnBeginRow();
    }

    void OnValue(TUnversionedValue value, const TProtobufFieldDescription& fieldDescription)
    {
        if (fieldDescription.Repeated || fieldDescription.Type == EProtobufType::StructuredMessage) {
            ValidateUnversionedValueType(value, EValueType::Any);
            TMemoryInput input(value.Data.String, value.Length);
            TYsonPullParser parser(&input, EYsonType::Node);
            TYsonPullParserCursor cursor(&parser);
            TotalSize_ += Traverse(fieldDescription, &cursor, /* repeatedProcessed */ false);
        } else if (fieldDescription.Type == EProtobufType::Any) {
            TotalSize_ += ProcessAnyValue(
                fieldDescription,
                [value] (TYsonWriter& writer) {
                    UnversionedValueToYson(value, &writer);
                });
        } else {
            TotalSize_ += ProcessSimpleType(fieldDescription, TUnversionedValueGetter(value));
        }
    }

    void OnUnknownValue(TUnversionedValue value)
    {
        OtherColumnsWriter_.OnValue(value);
    }

    void OnEndRow()
    {
        OtherColumnsWriter_.OnEndRow();
        TotalSize_ += OtherColumnsWriter_.GetProtobufSize();
    }

    void WriteMessage(IOutputStream* stream)
    {
        WritePod(*stream, static_cast<ui32>(TotalSize_));
        for (const auto& resultNode : Result_) {
            Visit(resultNode,
                [&] (const THeaderResultNode& headerNode) {
                    WriteVarUint32(stream, static_cast<ui32>(headerNode.WireTag));
                    WriteVarUint32(stream, static_cast<ui32>(headerNode.Size));
                },
                [&] (const TDataResultNode& dataNode) {
                    stream->Write(
                        Buffer_.data() + dataNode.Begin,
                        static_cast<size_t>(dataNode.Size));
                });
        }
        OtherColumnsWriter_.WriteProtoField(stream);
    }

private:
    struct THeaderResultNode
    {
        ui64 WireTag;
        i64 Size;
    };

    struct TDataResultNode
    {
        i64 Begin;
        i64 Size;
    };

    using TResultNode = std::variant<
        THeaderResultNode,
        TDataResultNode>;

private:
    i64 Traverse(
        const TProtobufFieldDescription& fieldDescription,
        TYsonPullParserCursor* cursor,
        bool repeatedProcessed)
    {
        auto actuallyRepeated = fieldDescription.Repeated && !repeatedProcessed;
        if (actuallyRepeated) {
            YT_VERIFY(!fieldDescription.Optional);
            ValidateYsonCursorType(cursor, EYsonItemType::BeginList);
            cursor->Next();
            i64 size = 0;
            while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                size += Traverse(fieldDescription, cursor, /* repeatedProcessed */ true);
            }
            cursor->Next();
            return size;
        }

        if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
            if (Y_UNLIKELY(!fieldDescription.Optional)) {
                THROW_ERROR_EXCEPTION("Expected non-optional protobuf field %Qv of type %Qlv, "
                    "got YSON \"entity\" item",
                    fieldDescription.Name,
                    fieldDescription.Type);
            }
            cursor->Next();
            return 0;
        }

        switch (fieldDescription.Type) {
            case EProtobufType::StructuredMessage: {
                auto headerResultNodeIndex = Result_.size();
                Result_.emplace_back(THeaderResultNode{});

                ValidateYsonCursorType(cursor, EYsonItemType::BeginList);
                int elementIndex = 0;
                cursor->Next();
                auto childIterator = fieldDescription.Children.cbegin();
                i64 fieldsSize = 0;
                while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                    if (childIterator != fieldDescription.Children.cend() && childIterator->StructElementIndex == elementIndex) {
                        fieldsSize += Traverse(*childIterator, cursor, /* repeatedProcessed */ false);
                        ++childIterator;
                    } else {
                        cursor->SkipComplexValue();
                    }
                    ++elementIndex;
                }
                cursor->Next();

                auto& headerResultNode = std::get<THeaderResultNode>(Result_[headerResultNodeIndex]);
                headerResultNode.WireTag = fieldDescription.WireTag;
                headerResultNode.Size = fieldsSize;
                auto fieldsSizeSize = static_cast<i64>(WireFormatLite::Int64Size(fieldsSize));
                return fieldDescription.TagSize + fieldsSizeSize + fieldsSize;
            }
            case EProtobufType::Any:
                return ProcessAnyValue(
                    fieldDescription,
                    [&] (TYsonWriter& writer) {
                        cursor->TransferComplexValue(&writer);
                    });
            default: {
                auto size = ProcessSimpleType(fieldDescription, TYsonValueGetter(cursor));
                cursor->Next();
                return size;
            }
        }
    }

    template <typename TCallback>
    i64 ProcessAnyValue(const TProtobufFieldDescription& fieldDescription, TCallback callback)
    {
        auto headerResultNodeIndex = Result_.size();
        Result_.emplace_back(THeaderResultNode{});
        auto& dataResultNode = std::get<TDataResultNode>(Result_.emplace_back(TDataResultNode{}));
        auto originalBufferSize = static_cast<i64>(Buffer_.size());

        TYsonWriter writer(&BufferOutput_, EYsonFormat::Binary, EYsonType::Node, /* enableRaw */ true);
        callback(writer);

        auto writtenByteCount = static_cast<i64>(Buffer_.size()) - originalBufferSize;
        dataResultNode.Begin = originalBufferSize;
        dataResultNode.Size = writtenByteCount;

        auto& headerResultNode = std::get<THeaderResultNode>(Result_[headerResultNodeIndex]);
        headerResultNode.WireTag = fieldDescription.WireTag;
        headerResultNode.Size = writtenByteCount;
        auto writtenByteCountSize = static_cast<i64>(WireFormatLite::Int64Size(writtenByteCount));
        return fieldDescription.TagSize + writtenByteCountSize + writtenByteCount;
    }

    template <typename TValueGetter>
    i64 ProcessSimpleType(const TProtobufFieldDescription& fieldDescription, const TValueGetter& valueGetter)
    {
        auto& dataResultNode = GetOrCreateDataResultNode();
        auto originalBufferSize = static_cast<i64>(Buffer_.size());
        WriteVarUint32(&BufferOutput_, fieldDescription.WireTag);
        WriteProtobufField(&BufferOutput_, fieldDescription, valueGetter);
        auto writtenByteCount = static_cast<i64>(Buffer_.size()) - originalBufferSize;
        dataResultNode.Size += writtenByteCount;
        return writtenByteCount;
    }

    TDataResultNode& GetOrCreateDataResultNode()
    {
        if (Result_.empty() || !std::holds_alternative<TDataResultNode>(Result_.back())) {
            TDataResultNode node;
            node.Begin = Buffer_.size();
            node.Size = 0;
            Result_.push_back(node);
        }
        return std::get<TDataResultNode>(Result_.back());
    }

private:
    TOtherColumnsWriter OtherColumnsWriter_;
    i64 TotalSize_ = 0;
    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    std::vector<TResultNode> Result_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForProtobuf
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterForProtobuf(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TProtobufFormatDescriptionPtr description)
        : TSchemalessFormatWriterBase(
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount)
        , Description_(description)
        , WriterImpl_(nameTable, description)
    {
        TableIndexToFieldIndexToDescription_.resize(Description_->GetTableCount());
        WriterImpl_.SetTableIndex(CurrentTableIndex_);
    }

private:
    virtual void DoWrite(TRange<TUnversionedRow> rows) override
    {
        auto stream = GetOutputStream();

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; ++index) {
            auto row = rows[index];

            if (CheckKeySwitch(row, index + 1 == rowCount)) {
                WritePod(*stream, LenvalKeySwitch);
            }

            WriteControlAttributes(row);

            WriterImpl_.OnBeginRow();
            for (const auto& value : row) {
                const auto* fieldDescription = GetFieldDescription(
                    CurrentTableIndex_,
                    value.Id,
                    NameTable_);

                if (!fieldDescription) {
                    WriterImpl_.OnUnknownValue(value);
                    continue;
                }

                if (value.Type == EValueType::Null) {
                    continue;
                }

                try {
                    WriterImpl_.OnValue(value, *fieldDescription);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error writing value of field %Qv",
                        fieldDescription->Name)
                        << ex;
                }
            }
            WriterImpl_.OnEndRow();
            WriterImpl_.WriteMessage(stream);
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    virtual void WriteTableIndex(i64 tableIndex) override
    {
        CurrentTableIndex_ = tableIndex;
        WriterImpl_.SetTableIndex(tableIndex);

        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalTableIndexMarker));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    }

    virtual void WriteRangeIndex(i64 rangeIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalRangeIndexMarker));
        WritePod(*stream, static_cast<ui32>(rangeIndex));
    }

    virtual void WriteRowIndex(i64 rowIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalRowIndexMarker));
        WritePod(*stream, static_cast<ui64>(rowIndex));
    }

    const TProtobufFieldDescription* GetFieldDescription(ui32 tableIndex, ui32 fieldIndex, const NTableClient::TNameTablePtr& nameTable)
    {
        if (Y_UNLIKELY(tableIndex >= TableIndexToFieldIndexToDescription_.size())) {
            THROW_ERROR_EXCEPTION("Table with index %v is missing in format description",
                tableIndex);
        }
        auto& fieldIndexToDescription = TableIndexToFieldIndexToDescription_[tableIndex];
        if (fieldIndexToDescription.size() <= fieldIndex) {
            const auto& tableDescription = Description_->GetTableDescription(tableIndex);
            fieldIndexToDescription.reserve(fieldIndex + 1);
            for (size_t i = fieldIndexToDescription.size(); i <= fieldIndex; ++i) {
                YT_ASSERT(fieldIndexToDescription.size() == i);
                auto fieldName = nameTable->GetName(i);
                auto it = tableDescription.Columns.find(fieldName);
                if (it == tableDescription.Columns.end()) {
                    fieldIndexToDescription.push_back(nullptr);
                } else {
                    fieldIndexToDescription.push_back(&it->second);
                }
            }
        }
        return fieldIndexToDescription[fieldIndex];
    }

private:
    const TProtobufFormatDescriptionPtr Description_;
    std::vector<std::vector<const TProtobufFieldDescription*>> TableIndexToFieldIndexToDescription_;
    TWriterImpl WriterImpl_;

    int CurrentTableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForProtobuf(
    TProtobufFormatConfigPtr config,
    const std::vector<TTableSchema>& schemas,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto description = New<TProtobufFormatDescription>();
    description->Init(config, schemas, /* validateMissingFieldsOptionality */ false);
    return New<TSchemalessWriterForProtobuf>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        std::move(description));
}

ISchemalessFormatWriterPtr CreateWriterForProtobuf(
    const IAttributeDictionary& attributes,
    const std::vector<TTableSchema>& schemas,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(&attributes);
    return CreateWriterForProtobuf(
        config,
        schemas,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
