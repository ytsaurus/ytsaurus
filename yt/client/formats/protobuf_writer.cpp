#include "protobuf_writer.h"

#include "lenval_control_constants.h"
#include "protobuf.h"
#include "schemaless_writer_adapter.h"
#include "unversioned_value_yson_writer.h"

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/misc/varint.h>
#include <yt/core/misc/zerocopy_output_writer.h>

#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/token_writer.h>

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
        const std::vector<TTableSchema>& schemas,
        const TNameTablePtr& nameTable,
        const TProtobufFormatDescriptionPtr& description,
        EComplexTypeMode complexTypeMode)
        : NameTableReader_(nameTable)
        , TableIndexToOtherColumnsField_(description->GetTableCount())
        , TableIndexToConverter_(description->GetTableCount())
        , Writer_(
            &OutputStream_,
            EYsonFormat::Binary,
            EYsonType::Node,
            /* enableRaw */ true)
    {
        for (size_t tableIndex = 0; tableIndex < description->GetTableCount(); ++tableIndex) {
            const auto& tableDescription = description->GetTableDescription(tableIndex);
            for (const auto& [name, fieldDescription] : tableDescription.Columns) {
                if (fieldDescription.Type == EProtobufType::OtherColumns) {
                    TableIndexToOtherColumnsField_[tableIndex] = fieldDescription;
                    TableIndexToConverter_[tableIndex].emplace(
                        nameTable,
                        schemas[tableIndex],
                        complexTypeMode,
                        /* skipNullValues */ false);
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
        if (TableIndexToOtherColumnsField_[tableIndex]) {
            FieldDescription_ = &*TableIndexToOtherColumnsField_[tableIndex];
            Converter_ = &*TableIndexToConverter_[tableIndex];
        } else {
            FieldDescription_ = nullptr;
            Converter_ = nullptr;
        }
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
        Converter_->WriteValue(value, &Writer_);
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

    void WriteProtoField(TZeroCopyOutputStreamWriter* writer) const
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(!InsideRow_);
        WriteVarUint32(writer, FieldDescription_->WireTag);
        auto buffer = GetYsonString();
        WriteVarUint32(writer, buffer.size());
        writer->Write(buffer.begin(), buffer.size());
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

    std::vector<std::optional<TProtobufFieldDescription>> TableIndexToOtherColumnsField_;
    const TProtobufFieldDescription* FieldDescription_ = nullptr;

    std::vector<std::optional<TUnversionedValueYsonWriter>> TableIndexToConverter_;
    TUnversionedValueYsonWriter* Converter_ = nullptr;

    TBlobOutput OutputStream_;
    TYsonWriter Writer_;

    bool InsideRow_ = false;

    int RowIndexColumnId_ = -1;
    int RangeIndexColumnId_ = -1;
    int TableIndexColumnId_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

class TEnumVisitor
{
public:
    Y_FORCE_INLINE void OnInt64(i64 value)
    {
        InRange = TryIntegralCast<i32>(value, &EnumValue);
    }

    Y_FORCE_INLINE void OnUint64(ui64 value)
    {
        InRange = TryIntegralCast<i32>(value, &EnumValue);
    }

    Y_FORCE_INLINE void OnString(TStringBuf value, const TProtobufFieldDescription& fieldDescription)
    {
        if (Y_UNLIKELY(!fieldDescription.EnumerationDescription)) {
            THROW_ERROR_EXCEPTION("Enumeration description not found for field %Qv",
                fieldDescription.Name);
        }
        EnumValue = fieldDescription.EnumerationDescription->GetValue(value);
    }

public:
    bool InRange = true;
    i32 EnumValue;
};

template <typename TValueExtractor>
Y_FORCE_INLINE void WriteProtobufField(
    TZeroCopyOutputStreamWriter* writer,
    const TProtobufFieldDescription& fieldDescription,
    const TValueExtractor& extractor)
{
    switch (fieldDescription.Type) {
        case EProtobufType::String:
        case EProtobufType::Bytes:
        case EProtobufType::Message: {
            auto stringBuf = extractor.ExtractString();
            WriteVarUint32(writer, stringBuf.size());
            writer->Write(stringBuf.data(), stringBuf.size());
            return;
        }
        case EProtobufType::Uint64:
            WriteVarUint64(writer, extractor.ExtractUint64());
            return;
        case EProtobufType::Uint32:
            WriteVarUint32(writer, extractor.ExtractUint64());
            return;
        case EProtobufType::Int64:
            WriteVarUint64(writer, extractor.ExtractInt64()); // no zigzag
            return;
        case EProtobufType::Int32:
            WriteVarUint64(writer, extractor.ExtractInt64()); // no zigzag
            return;
        case EProtobufType::Sint64:
            WriteVarInt64(writer, extractor.ExtractInt64()); // zigzag
            return;
        case EProtobufType::Sint32:
            WriteVarInt32(writer, extractor.ExtractInt64()); // zigzag
            return;
        case EProtobufType::Fixed64:
            WritePod(*writer, extractor.ExtractUint64());
            return;
        case EProtobufType::Fixed32:
            WritePod(*writer, static_cast<ui32>(extractor.ExtractUint64()));
            return;
        case EProtobufType::Sfixed64:
            WritePod(*writer, extractor.ExtractInt64());
            return;
        case EProtobufType::Sfixed32:
            WritePod(*writer, static_cast<i32>(extractor.ExtractInt64()));
            return;
        case EProtobufType::Double:
            WritePod(*writer, extractor.ExtractDouble());
            return;
        case EProtobufType::Float:
            WritePod(*writer, static_cast<float>(extractor.ExtractDouble()));
            return;
        case EProtobufType::Bool:
            WritePod(*writer, static_cast<ui8>(extractor.ExtractBoolean()));
            return;
        case EProtobufType::EnumInt:
        case EProtobufType::EnumString: {
            auto getEnumerationName = [&] () {
                return fieldDescription.EnumerationDescription
                    ? fieldDescription.EnumerationDescription->GetEnumerationName()
                    : "<unknown>";
            };
            TEnumVisitor visitor;
            extractor.ExtractEnum(&visitor, fieldDescription);
            if (Y_UNLIKELY(!visitor.InRange)) {
                THROW_ERROR_EXCEPTION("Value out of range for protobuf enumeration %Qv",
                    getEnumerationName());
            }
            WriteVarUint64(writer, static_cast<ui64>(visitor.EnumValue)); // No zigzag int32.
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

class TYsonValueExtractor
{
public:
    explicit TYsonValueExtractor(TYsonPullParser* parser)
        : Parser_(parser)
    { }

    void ExtractEnum(TEnumVisitor* visitor, const TProtobufFieldDescription& fieldDescription) const
    {
        auto item = Parser_->Next();
        switch (item.GetType()) {
            case EYsonItemType::Int64Value:
                visitor->OnInt64(item.UncheckedAsInt64());
                return;
            case EYsonItemType::Uint64Value:
                visitor->OnUint64(item.UncheckedAsUint64());
                return;
            case EYsonItemType::StringValue:
                visitor->OnString(item.UncheckedAsString(), fieldDescription);
                return;
            default:
                auto* enumDescription = fieldDescription.EnumerationDescription;
                THROW_ERROR_EXCEPTION("Cannot parse protobuf enumeration %Qv from YSON value of type %Qlv",
                    enumDescription ? enumDescription->GetEnumerationName() : "<unknown>",
                    item.GetType());
        }
    }

    i64 ExtractInt64() const
    {
        return Parser_->ParseInt64();
    }

    ui64 ExtractUint64() const
    {
        return Parser_->ParseUint64();
    }

    TStringBuf ExtractString() const
    {
        return Parser_->ParseString();
    }

    bool ExtractBoolean() const
    {
        return Parser_->ParseBoolean();
    }

    double ExtractDouble() const
    {
        return Parser_->ParseDouble();
    }

private:
    TYsonPullParser* const Parser_;
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

// This class actually doesn't change the `TUnversionedValue` passed to it,
// but is named consistently with more tricky `TYsonValueExtractor`.
class TUnversionedValueExtractor
{
public:
    explicit TUnversionedValueExtractor(TUnversionedValue value)
        : Value_(value)
    { }

    void ExtractEnum(TEnumVisitor* visitor, const TProtobufFieldDescription& fieldDescription) const
    {
        switch (Value_.Type) {
            case EValueType::Int64:
                visitor->OnInt64(Value_.Data.Int64);
                return;
            case EValueType::Uint64:
                visitor->OnUint64(Value_.Data.Uint64);
                return;
            case EValueType::String:
                visitor->OnString(TStringBuf(Value_.Data.String, Value_.Length), fieldDescription);
                return;
            default:
                auto* enumDescription = fieldDescription.EnumerationDescription;
                THROW_ERROR_EXCEPTION("Cannot parse protobuf enumeration %Qv from unverioned value of type %Qlv",
                    enumDescription ? enumDescription->GetEnumerationName() : "<unknown>",
                    Value_.Type);
        }
    }

    i64 ExtractInt64() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Int64);
        return Value_.Data.Int64;
    }

    ui64 ExtractUint64() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Uint64);
        return Value_.Data.Uint64;
    }

    TStringBuf ExtractString() const
    {
        ValidateUnversionedValueType(Value_, EValueType::String);
        return {Value_.Data.String, Value_.Length};
    }

    bool ExtractBoolean() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Boolean);
        return Value_.Data.Boolean;
    }

    double ExtractDouble() const
    {
        ValidateUnversionedValueType(Value_, EValueType::Double);
        return Value_.Data.Double;
    }

private:
    const TUnversionedValue Value_;
};

////////////////////////////////////////////////////////////////////////////////

// Write varint reprsentation occupying exactly `size` bytes.
// If `value` is too small, `0x80` bytes will be added in due amount.
int WriteVarUint64WithPadding(char* output, ui64 value, int size)
{
    for (int i = 0; i < size - 1; ++i) {
        *output++ = static_cast<ui8>(value | 0x80);
        value >>= 7;
    }
    *output++ = static_cast<ui8>(value);
    YT_VERIFY(value < 0x80);
    return size;
}

class TZeroCopyWriterWithGapsBase
{
public:
    TZeroCopyWriterWithGapsBase(TBlob& blob)
        : Blob_(blob)
        , InitialSize_(blob.Size())
    { }

protected:
    TBlob& Blob_;
    ui64 InitialSize_;
};

// Same as `TZeroCopyOutputStreamWriter` but also allows leaving small "gaps"
// of fixed size in the output blob to be filled afterwards.
//
// Example usage:
// ```
//   auto gap = writer->CreateGap(sizeof(ui64));
//   auto writtenSizeBefore = writer->GetTotalWrittenSize();
//   ...  // Write something to the writer.
//   auto writtenSizeAfter = writer->GetTotalWrittenSize();
//   ui64 size = writtenSizeAfter - writtenSizeBefore;
//   memcpy(writer->GetGapPointer(gap), &size, sizeof(size));
// ```
class TZeroCopyWriterWithGaps
    : public TZeroCopyWriterWithGapsBase
    , public TZeroCopyOutputStreamWriter
{
public:
    static constexpr ui64 MaxGapSize = 16;

    using TGapPosition = ui64;

    // NOTE: We need base class to initialize `InitialSize_` before `TZeroCopyOutputStreamWriter`.
    TZeroCopyWriterWithGaps(TBlobOutput* blobOutput)
        : TZeroCopyWriterWithGapsBase(blobOutput->Blob())
        , TZeroCopyOutputStreamWriter(blobOutput)
    { }

    TGapPosition CreateGap(ui64 size)
    {
        auto position = InitialSize_ + GetTotalWrittenSize();
        if (size <= RemainingBytes()) {
            Advance(size);
        } else {
            char Buffer[MaxGapSize];
            YT_VERIFY(size <= MaxGapSize);
            Write(Buffer, size);
        }
        return position;
    }

    char* GetGapPointer(TGapPosition gap)
    {
        return Blob_.Begin() + gap;
    }
};

class TWriterImpl
{
private:
    using TMessageSize = ui32;

public:
    TWriterImpl(
        const std::vector<TTableSchema>& schemas,
        const TNameTablePtr& nameTable,
        const TProtobufFormatDescriptionPtr& description,
        EComplexTypeMode complexTypeMode)
        : OtherColumnsWriter_(schemas, nameTable, description, complexTypeMode)
    { }

    void SetTableIndex(i64 tableIndex)
    {
        OtherColumnsWriter_.SetTableIndex(tableIndex);
    }

    Y_FORCE_INLINE void OnBeginRow(TZeroCopyWriterWithGaps* writer)
    {
        Writer_ = writer;
        MessageSizeGapPosition_ = Writer_->CreateGap(sizeof(TMessageSize));
        OtherColumnsWriter_.OnBeginRow();
        TotalWrittenSizeBefore_ = Writer_->GetTotalWrittenSize();
    }

    // It's quite likely that this value can be bounded by `WireFormatLite::UInt64Size(10 * ysonLength)`,
    // but currently we return just maximum size of a varint representation of a 32-bit number.
    static Y_FORCE_INLINE int GetMaxVarIntSizeOfProtobufSizeOfComplexType(ui64 ysonLength)
    {
        return MaxVarUint32Size;
    }

    static Y_FORCE_INLINE ui64 GetMaxBinaryYsonSize(TUnversionedValue value)
    {
        switch (value.Type) {
            case EValueType::Uint64:
                return 1 + MaxVarUint64Size;
            case EValueType::Int64:
                return 1 + MaxVarInt64Size;
            case EValueType::Double:
                return 1 + sizeof(double);
            case EValueType::Boolean:
                return 1;
            case EValueType::String:
                return 1 + MaxVarInt32Size + value.Length;
            case EValueType::Any:
                return value.Length;
            case EValueType::Null:
                return 1;
            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
                YT_ABORT();
        }
        YT_ABORT();
    }

    Y_FORCE_INLINE void OnValue(TUnversionedValue value, const TProtobufFieldDescription& fieldDescription)
    {
        if (fieldDescription.Repeated || fieldDescription.Type == EProtobufType::StructuredMessage) {
            ValidateUnversionedValueType(value, EValueType::Any);
            TMemoryInput input(value.Data.String, value.Length);
            TYsonPullParser parser(&input, EYsonType::Node);
            auto maxVarIntSize = GetMaxVarIntSizeOfProtobufSizeOfComplexType(value.Length);
            Traverse(fieldDescription, &parser, maxVarIntSize);
        } else {
            WriteVarUint32(Writer_, fieldDescription.WireTag);
            if (fieldDescription.Type == EProtobufType::Any) {
                auto maxYsonSize = GetMaxBinaryYsonSize(value);
                WriteWithSizePrefix(WireFormatLite::UInt64Size(maxYsonSize), [&] {
                    TCheckedInDebugYsonTokenWriter tokenWriter(Writer_);
                    UnversionedValueToYson(value, &tokenWriter);
                });
            } else {
                WriteProtobufField(Writer_, fieldDescription, TUnversionedValueExtractor(value));
            }
        }
    }

    Y_FORCE_INLINE void OnUnknownValue(TUnversionedValue value)
    {
        OtherColumnsWriter_.OnValue(value);
    }

    Y_FORCE_INLINE void OnEndRow()
    {
        OtherColumnsWriter_.OnEndRow();
        OtherColumnsWriter_.WriteProtoField(Writer_);
        Writer_->UndoRemaining();
        auto totalWrittenSizeAfter = Writer_->GetTotalWrittenSize();
        auto messageSize = totalWrittenSizeAfter - TotalWrittenSizeBefore_;
        if (messageSize >= std::numeric_limits<TMessageSize>::max()) {
            THROW_ERROR_EXCEPTION("Too large protobuf message: limit is %v, actual size is %v",
                std::numeric_limits<TMessageSize>::max(),
                messageSize);
        }
        auto messageSizeCast = static_cast<TMessageSize>(messageSize);
        memcpy(Writer_->GetGapPointer(MessageSizeGapPosition_), &messageSizeCast, sizeof(messageSizeCast));
    }

private:
    void Traverse(
        const TProtobufFieldDescription& fieldDescription,
        TYsonPullParser* parser,
        int maxVarIntSize)
    {
        if (fieldDescription.Repeated) {
            YT_VERIFY(!fieldDescription.Optional);
            if (fieldDescription.Packed) {
                TraversePackedRepeated(fieldDescription, parser, maxVarIntSize);
            } else {
                parser->ParseBeginList();
                while (!parser->IsEndList()) {
                    TraverseNonRepeated(fieldDescription, parser, maxVarIntSize);
                }
                parser->ParseEndList();
            }
        } else {
            TraverseNonRepeated(fieldDescription, parser, maxVarIntSize);
        }
    }

    template <typename TFun>
    Y_FORCE_INLINE void WriteWithSizePrefix(int maxVarIntSize, TFun writerFun)
    {
        auto gap = Writer_->CreateGap(maxVarIntSize);
        auto totalWrittenSizeBefore = Writer_->GetTotalWrittenSize();

        writerFun();

        auto totalWrittenSizeAfter = Writer_->GetTotalWrittenSize();
        auto messageSize = totalWrittenSizeAfter - totalWrittenSizeBefore;
        WriteVarUint64WithPadding(Writer_->GetGapPointer(gap), messageSize, maxVarIntSize);
    }

    Y_FORCE_INLINE void TraversePackedRepeated(
        const TProtobufFieldDescription& fieldDescription,
        TYsonPullParser* parser,
        int maxVarIntSize)
    {
        parser->ParseBeginList();
        if (!parser->IsEndList()) {
            WriteVarUint32(Writer_, fieldDescription.WireTag);
            WriteWithSizePrefix(maxVarIntSize, [&] {
                while (!parser->IsEndList()) {
                    WriteProtobufField(Writer_, fieldDescription, TYsonValueExtractor(parser));
                }
            });
        }
        parser->ParseEndList();
    }

    Y_FORCE_INLINE void TraverseNonRepeated(
        const TProtobufFieldDescription& fieldDescription,
        TYsonPullParser* parser,
        int maxVarIntSize)
    {
        if (fieldDescription.Optional && parser->IsEntity()) {
            parser->ParseEntity();
            if (fieldDescription.Type == EProtobufType::Any) {
                WriteVarUint32(Writer_, fieldDescription.WireTag);
                WriteWithSizePrefix(1, [&] {
                    TCheckedInDebugYsonTokenWriter writer(Writer_);
                    writer.WriteEntity();
                });
            }
            return;
        }

        WriteVarUint32(Writer_, fieldDescription.WireTag);
        switch (fieldDescription.Type) {
            case EProtobufType::StructuredMessage:
                WriteWithSizePrefix(maxVarIntSize, [&] {
                    WriteStruct(fieldDescription, parser, maxVarIntSize);
                });
                return;
            case EProtobufType::Any:
                WriteWithSizePrefix(maxVarIntSize, [&] {
                    TCheckedInDebugYsonTokenWriter writer(Writer_);
                    parser->TransferComplexValue(&writer);
                });
                return;
            default:
                WriteProtobufField(Writer_, fieldDescription, TYsonValueExtractor(parser));
                return;
        }
        YT_ABORT();
    }

    Y_FORCE_INLINE void WriteStruct(
        const TProtobufFieldDescription& fieldDescription,
        TYsonPullParser* parser,
        int maxVarIntSize)
    {
        parser->ParseBeginList();
        auto childIterator = fieldDescription.Children.cbegin();
        int elementIndex = 0;
        while (!parser->IsEndList()) {
            if (childIterator == fieldDescription.Children.cend() || childIterator->StructElementIndex != elementIndex) {
                parser->SkipComplexValue();
                ++elementIndex;
                continue;
            }
            if (childIterator->Repeated) {
                Traverse(*childIterator, parser, maxVarIntSize);
            } else {
                TraverseNonRepeated(*childIterator, parser, maxVarIntSize);
            }
            ++childIterator;
            ++elementIndex;
        }
        parser->ParseEndList();
    }

private:
    TOtherColumnsWriter OtherColumnsWriter_;
    TZeroCopyWriterWithGaps* Writer_;
    TZeroCopyWriterWithGaps::TGapPosition MessageSizeGapPosition_;
    ui64 TotalWrittenSizeBefore_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForProtobuf
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterForProtobuf(
        const std::vector<TTableSchema>& schemas,
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TProtobufFormatDescriptionPtr description,
        EComplexTypeMode complexTypeMode)
        : TSchemalessFormatWriterBase(
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount)
        , Description_(description)
        , WriterImpl_(schemas, nameTable, description, complexTypeMode)
        , StreamWriter_(GetOutputStream())
    {
        TableIndexToFieldIndexToDescription_.resize(Description_->GetTableCount());
        WriterImpl_.SetTableIndex(CurrentTableIndex_);
    }

private:
    virtual void DoWrite(TRange<TUnversionedRow> rows) override
    {
        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; ++index) {
            auto row = rows[index];

            if (CheckKeySwitch(row, index + 1 == rowCount)) {
                WritePod(*StreamWriter_, LenvalKeySwitch);
            }

            WriteControlAttributes(row);

            WriterImpl_.OnBeginRow(&*StreamWriter_);
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
            TryFlushBufferAndUpdateWriter(false);
        }
        TryFlushBufferAndUpdateWriter(true);
    }

    void TryFlushBufferAndUpdateWriter(bool force)
    {
        TryFlushBuffer(force);
        // |StreamWriter_| could have been reset in |FlushWriter()|.
        if (!StreamWriter_) {
            StreamWriter_.emplace(GetOutputStream());
        }
    }

    virtual void FlushWriter() override
    {
        // Reset |StreamWriter_| to ensure it will never touch the
        // underlying |TBlobOutput| as it will be |Flush()|-ed soon.
        StreamWriter_.reset();
        TSchemalessFormatWriterBase::FlushWriter();
    }

    virtual void WriteTableIndex(i64 tableIndex) override
    {
        CurrentTableIndex_ = tableIndex;
        WriterImpl_.SetTableIndex(tableIndex);

        WritePod(*StreamWriter_, static_cast<ui32>(LenvalTableIndexMarker));
        WritePod(*StreamWriter_, static_cast<ui32>(tableIndex));
    }

    virtual void WriteRangeIndex(i64 rangeIndex) override
    {
        WritePod(*StreamWriter_, static_cast<ui32>(LenvalRangeIndexMarker));
        WritePod(*StreamWriter_, static_cast<ui32>(rangeIndex));
    }

    virtual void WriteRowIndex(i64 rowIndex) override
    {
        WritePod(*StreamWriter_, static_cast<ui32>(LenvalRowIndexMarker));
        WritePod(*StreamWriter_, static_cast<ui64>(rowIndex));
    }

    const TProtobufFieldDescription* GetFieldDescription(
        ui32 tableIndex,
        ui32 fieldIndex,
        const TNameTablePtr& nameTable)
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

    // Use optional to be able to destruct underlying object when switching output streams.
    std::optional<TZeroCopyWriterWithGaps> StreamWriter_;

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
        schemas,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        std::move(description),
        config->ComplexTypeMode);
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
