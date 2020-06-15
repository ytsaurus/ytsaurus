#include "arrow_row_stream_encoder.h"

#include <yt/client/arrow/fbs/Message.fbs.h>
#include <yt/client/arrow/fbs/Schema.fbs.h>

#include <yt/client/api/rpc_proxy/row_stream.h>
#include <yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/unversioned_row_batch.h>

#include <yt/ytlib/table_client/columnar.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/range.h>

#include <util/system/align.h>

namespace NYT::NArrow {

using namespace NApi::NRpcProxy;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ArrowLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

using TBatchColumn = IUnversionedColumnarRowBatch::TColumn;
using TBodyWriter = std::function<void(TMutableRef)>;

constexpr i64 ArrowAlignment = 8;

i64 GetBytesPerValue(const TBatchColumn& column)
{
    const auto& values = *column.Values;
    if (values.BitWidth == 1) {
        return 1;
    } else {
        YT_VERIFY(values.BitWidth >= 8 && IsPowerOf2(values.BitWidth));
        return values.BitWidth / 8;
    }
}

i64 GetTotalValuesSize(const TBatchColumn& column, int count)
{
    return AlignUp<i64>(GetBytesPerValue(column) * count, ArrowAlignment);
}

TLogicalTypePtr DropOptional(TLogicalTypePtr type)
{
    return type->GetMetatype() == ELogicalMetatype::Optional
        ? type->AsOptionalTypeRef().GetElement()
        : type;
}

flatbuffers::Offset<flatbuffers::String> SerializeString(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TString& str)
{
    return flatbufBuilder->CreateString(str.data(), str.length());
}

std::tuple<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>> SerializeColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TColumnSchema& schema)
{
    auto type = DropOptional(schema.LogicalType());
    if (type->GetMetatype() != ELogicalMetatype::Simple) {
        THROW_ERROR_EXCEPTION("Column %Qv has metatype %Qlv that is not currently supported by Arrow encoder",
            schema.Name(),
            type->GetMetatype());
    }

    auto simpleType = type->AsSimpleTypeRef().GetElement();
    switch (simpleType) {
        case ESimpleLogicalValueType::Null:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Null,
                org::apache::arrow::flatbuf::CreateNull(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Int,
                org::apache::arrow::flatbuf::CreateInt(
                    *flatbufBuilder,
                    GetIntegralTypeBitWidth(simpleType),
                    IsIntegralTypeSigned(simpleType)).Union());

        case ESimpleLogicalValueType::Double:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_FloatingPoint,
                org::apache::arrow::flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::Precision_DOUBLE)
                    .Union());

        case ESimpleLogicalValueType::Boolean:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Bool,
                org::apache::arrow::flatbuf::CreateBool(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Binary,
                org::apache::arrow::flatbuf::CreateBinary(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::Utf8:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Utf8,
                org::apache::arrow::flatbuf::CreateUtf8(*flatbufBuilder)
                    .Union());

        default:
            THROW_ERROR_EXCEPTION("Column %Qv has type %Qlv that is not currently supported by Arrow encoder",
                schema.Name(),
                simpleType);
    }
}

bool IsRleButNotDictionaryEncodedStringLikeColumn(const TBatchColumn& column)
{
    auto type = DropOptional(column.Type);
    return
        type->GetMetatype() == ELogicalMetatype::Simple &&
        IsStringLikeType(type->AsSimpleTypeRef().GetElement()) &&
        column.Rle &&
        !column.Rle->ValueColumn->Dictionary;
}

bool IsRleDictionaryEncodedColumn(const TBatchColumn& column)
{
    return
        column.Rle &&
        column.Rle->ValueColumn->Dictionary;
}

bool IsDictionaryEncodedColumn(const TBatchColumn& column)
{
    return
        column.Dictionary ||
        IsRleDictionaryEncodedColumn(column) ||
        IsRleButNotDictionaryEncodedStringLikeColumn(column);
}

flatbuffers::Offset<org::apache::arrow::flatbuf::DictionaryEncoding> SerializeDictionaryEncoding(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TBatchColumn& column,
    int* dictionaryIdCounter)
{
    if (!IsDictionaryEncodedColumn(column)) {
        return {};
    }

    return org::apache::arrow::flatbuf::CreateDictionaryEncoding(
        *flatbufBuilder,
        *dictionaryIdCounter++);
}

struct TTypedBatchColumn
{
    const TBatchColumn* Column;
    TLogicalTypePtr Type;
};

struct TRecordBatchBodyPart
{
    i64 Size;
    TBodyWriter Writer;
};

struct TRecordBatchSerializationContext final
{
    explicit TRecordBatchSerializationContext(flatbuffers::FlatBufferBuilder* flatbufBuilder)
        : FlatbufBuilder(flatbufBuilder)
    { }

    void AddFieldNode(i64 length, i64 nullCount)
    {
        FieldNodes.emplace_back(length, nullCount);
    }
    
    void AddBuffer(i64 size, TBodyWriter writer)
    {
        YT_LOG_DEBUG("Buffer registered (Offset: %v, Size: %v)",
            CurrentBodyOffset,
            size);

        Buffers.emplace_back(CurrentBodyOffset, size);
        CurrentBodyOffset += AlignUp<i64>(size, ArrowAlignment);
        Parts.push_back(TRecordBatchBodyPart{size, std::move(writer)});
    }

    flatbuffers::FlatBufferBuilder* const FlatbufBuilder;

    i64 CurrentBodyOffset = 0;
    std::vector<org::apache::arrow::flatbuf::FieldNode> FieldNodes;
    std::vector<org::apache::arrow::flatbuf::Buffer> Buffers;
    std::vector<TRecordBatchBodyPart> Parts;
};

template <class T>
TRange<T> GetTypedValues(TRef ref)
{
    return MakeRange(
        reinterpret_cast<const T*>(ref.Begin()),
        reinterpret_cast<const T*>(ref.End()));
}

template <class T>
TMutableRange<T> GetTypedValues(TMutableRef ref)
{
    return MakeMutableRange(
        reinterpret_cast<T*>(ref.Begin()),
        reinterpret_cast<T*>(ref.End()));
}

template <class T>
TRange<T> GetTypedValues(const TBatchColumn& column)
{
    return GetTypedValues<T>(column.Values->Data);
}

template <class T>
TRange<T> GetRelevantTypedValues(const TBatchColumn& column)
{
    auto data = GetTypedValues<T>(column);
    return data.Slice(column.StartIndex, column.StartIndex + column.ValueCount);
}

void SerializeColumnPrologue(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    if (column->NullBitmap ||
        column->Rle && column->Rle->ValueColumn->NullBitmap)
    {
        if (column->Rle) {
            const auto* valueColumn = column->Rle->ValueColumn;
            auto rleIndexes = GetTypedValues<ui64>(*column);

            context->AddFieldNode(
                column->ValueCount,
                CountOnesInRleBitmap(
                    valueColumn->NullBitmap->Data,
                    rleIndexes,
                    column->StartIndex,
                    column->StartIndex + column->ValueCount));

            context->AddBuffer(
                GetBitmapByteSize(column->ValueCount),
                [=] (TMutableRef dstRef) {
                    BuildValidityBitmapFromRleNullBitmap(
                        valueColumn->NullBitmap->Data,
                        rleIndexes,
                        column->StartIndex,
                        column->StartIndex + column->ValueCount,
                        dstRef);
                });
        } else {
            context->AddFieldNode(
                column->ValueCount,
                CountOnesInBitmap(
                    column->NullBitmap->Data,
                    column->StartIndex,
                    column->StartIndex + column->ValueCount));

            context->AddBuffer(
                GetBitmapByteSize(column->ValueCount),
                [=] (TMutableRef dstRef) {
                    CopyBitmapRangeNegated(
                        column->NullBitmap->Data,
                        column->StartIndex,
                        column->StartIndex + column->ValueCount,
                        dstRef);
                });
        }
    } else {
        context->AddFieldNode(
            column->ValueCount,
            0);

        context->AddBuffer(
            0,
            [=] (TMutableRef /*dstRef*/) { });
    }
}

void SerializeRleButNotDictionaryEncodedStringLikeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    
    YT_LOG_DEBUG("Adding RLE- but not dictionary-encoded string-like column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount);

    SerializeColumnPrologue(typedColumn, context);

    auto rleIndexes = GetTypedValues<ui64>(*column);

    context->AddBuffer(
        sizeof (ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildIotaDictionaryIndexesFromRleIndexes(
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeDictionaryColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Dictionary->ZeroMeansNull);
    YT_VERIFY(column->Values->BitWidth == 32);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    
    YT_LOG_DEBUG("Adding dictionary column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    auto relevantDictionaryIndexes = GetRelevantTypedValues<ui32>(*column);

    context->AddFieldNode(
        column->ValueCount,
        CountNullsInDictionaryIndexesWithZeroNull(relevantDictionaryIndexes));

    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
                relevantDictionaryIndexes,
                dstRef);
        });
    
    context->AddBuffer(
        sizeof (ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
                relevantDictionaryIndexes,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeRleDictionaryColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    YT_VERIFY(column->Rle->ValueColumn->Dictionary->ZeroMeansNull);
    YT_VERIFY(column->Rle->ValueColumn->Values->BitWidth == 32);
    YT_VERIFY(column->Rle->ValueColumn->Values->BaseValue == 0);
    YT_VERIFY(!column->Rle->ValueColumn->Values->ZigZagEncoded);
    
    YT_LOG_DEBUG("Adding dictionary column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    auto dictionaryIndexes = GetTypedValues<ui32>(*column->Rle->ValueColumn);
    auto rleIndexes = GetTypedValues<ui64>(*column);

    context->AddFieldNode(
        column->ValueCount,
        CountNullsInRleDictionaryIndexesWithZeroNull(
            dictionaryIndexes,
            rleIndexes,
            column->StartIndex,
            column->StartIndex + column->ValueCount));

    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
                dictionaryIndexes,
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                dstRef);
        });
    
    context->AddBuffer(
        sizeof (ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
                dictionaryIndexes,
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeIntegerColumn(
    const TTypedBatchColumn& typedColumn,
    ESimpleLogicalValueType simpleType,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);

    YT_LOG_DEBUG("Adding integer column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    if (column->Rle) {
        YT_VERIFY(column->Values->BaseValue == 0);
        YT_VERIFY(column->Values->BitWidth == 64);
        YT_VERIFY(!column->Values->ZigZagEncoded);
        YT_VERIFY(column->Rle->ValueColumn->Values);
        YT_VERIFY(column->Rle->ValueColumn->Values->BitWidth == 64);
        
        context->AddBuffer(
            column->ValueCount * GetIntegralTypeByteSize(simpleType),
            [=] (TMutableRef dstRef) {
                auto values = GetTypedValues<ui64>(*column->Rle->ValueColumn);
                auto rleIndexes = GetTypedValues<ui64>(*column);
                auto dstValues = GetTypedValues<ui64>(dstRef);
                
                // TODO(babenko): merge these two calls
                DecodeRleVector(
                    values,
                    rleIndexes,
                    column->StartIndex,
                    column->StartIndex + column->ValueCount,
                    [] (ui64 value) { return value; },
                    dstValues);
                
                DecodeIntegerVector(
                    dstValues,
                    simpleType,
                    column->Rle->ValueColumn->Values->BaseValue,
                    column->Rle->ValueColumn->Values->ZigZagEncoded,
                    dstRef);
            });
    } else {
        YT_VERIFY(column->Values->BitWidth == 64);
        
        context->AddBuffer(
            column->ValueCount * GetIntegralTypeByteSize(simpleType),
            [=] (TMutableRef dstRef) {
                auto relevantValues = GetRelevantTypedValues<ui64>(*column);
                DecodeIntegerVector(
                    relevantValues,
                    simpleType,
                    column->Values->BaseValue,
                    column->Values->ZigZagEncoded,
                    dstRef);
            });
    }
}

void SerializeDoubleColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    
    YT_LOG_DEBUG("Adding double column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(double),
        [=] (TMutableRef dstRef) {
            auto relevantValues = GetRelevantTypedValues<double>(*column);
            ::memcpy(
                dstRef.Begin(),
                relevantValues.Begin(),
                column->ValueCount * sizeof(double));
        });
}

void SerializeStringLikeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(column->Values->BitWidth == 32);
    YT_VERIFY(column->Values->ZigZagEncoded);
    YT_VERIFY(column->Strings);
    YT_VERIFY(column->Strings->AvgLength);
    YT_VERIFY(!column->Rle);
    
    auto startIndex = column->StartIndex;
    auto endIndex = startIndex + column->ValueCount;
    auto stringData = column->Strings->Data;
    auto avgLength = *column->Strings->AvgLength;

    auto offsets = GetTypedValues<ui32>(*column);
    auto startOffset = DecodeStringOffset(offsets, avgLength, startIndex);
    auto endOffset = DecodeStringOffset(offsets, avgLength, endIndex);
    auto stringsSize = endOffset - startOffset;

    YT_LOG_DEBUG("Adding string-like column (ColumnId: %v, StartIndex: %v, ValueCount: %v, StartOffset: %v, EndOffset: %v, StringsSize: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        startOffset,
        endOffset,
        stringsSize);

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        sizeof(i32) * (column->ValueCount + 1),
        [=] (TMutableRef dstRef) {
            DecodeStringOffsets(
                offsets,
                avgLength,
                startIndex,
                endIndex,
                GetTypedValues<ui32>(dstRef));
         });

    context->AddBuffer(
        stringsSize,
        [=] (TMutableRef dstRef) {
            ::memcpy(
                dstRef.Begin(),
                stringData.Begin() + startOffset,
                stringsSize);
         });
}

void SerializeBooleanColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(column->Values->BitWidth == 1);

    YT_LOG_DEBUG("Adding boolean column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount);

    SerializeColumnPrologue(typedColumn, context);
    
    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            CopyBitmapRange(
                column->Values->Data,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                dstRef);
         });
}

void SerializeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;

    if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column)) {
        SerializeRleButNotDictionaryEncodedStringLikeColumn(typedColumn, context);
        return;
    }

    if (column->Dictionary) {
        SerializeDictionaryColumn(typedColumn, context);
        return;
    }

    if (column->Rle && column->Rle->ValueColumn->Dictionary) {
        SerializeRleDictionaryColumn(typedColumn, context);
        return;
    }

    auto type = DropOptional(typedColumn.Type);
    YT_VERIFY(type->GetMetatype() == ELogicalMetatype::Simple);
    auto simpleType = type->AsSimpleTypeRef().GetElement();

    if (IsIntegralType(simpleType)) {
        SerializeIntegerColumn(typedColumn, simpleType, context);
    } else if (simpleType == ESimpleLogicalValueType::Double) {
        SerializeDoubleColumn(typedColumn, context);
    } else if (IsStringLikeType(simpleType)) {
        SerializeStringLikeColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Boolean) {
        SerializeBooleanColumn(typedColumn, context);
    } else {
        YT_ABORT();
    }
}

auto SerializeRecordBatch(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    int length,
    TRange<TTypedBatchColumn> typedColumns)
{
    auto context = New<TRecordBatchSerializationContext>(flatbufBuilder);

    for (const auto& typedColumn : typedColumns) {
        SerializeColumn(typedColumn, context.Get());
    }

    auto fieldNodesOffset = flatbufBuilder->CreateVectorOfStructs(context->FieldNodes);

    auto buffersOffset = flatbufBuilder->CreateVectorOfStructs(context->Buffers);

    auto recordBatchOffset = org::apache::arrow::flatbuf::CreateRecordBatch(
        *flatbufBuilder,
        length,
        fieldNodesOffset,
        buffersOffset);

    auto totalSize = context->CurrentBodyOffset;

    return std::make_tuple(
        recordBatchOffset,
        totalSize,
        [context = std::move(context)] (TMutableRef dstRef) {
            char* current = dstRef.Begin();
            for (const auto& part : context->Parts) {
                part.Writer(TMutableRef(current, current + part.Size));
                current += AlignUp<i64>(part.Size, ArrowAlignment);
            }
            YT_VERIFY(current == dstRef.End());
        });
}

////////////////////////////////////////////////////////////////////////////////

class TArrowRowStreamBlockEncoder
{
public:
    TArrowRowStreamBlockEncoder(
        TTableSchemaPtr schema,
        TNameTablePtr nameTable,
        IUnversionedColumnarRowBatchPtr batch)
        : Schema_(std::move(schema))
        , NameTable_(std::move(nameTable))
        , Batch_(std::move(batch))
    {
        PrepareColumns();
        PrepareSchema();
        PrepareDictionaryBatches();
        PrepareRecordBatch();
    }

    i64 GetPayloadSize() const
    {
        i64 size = 0;
        for (const auto& message : Messages_) {
            size += sizeof (ui32); // continuation indicator
            size += sizeof (ui32); // metadata size
            size += AlignUp<i64>(message.FlatbufBuilder.GetSize(), ArrowAlignment); // metadata message
            size += AlignUp<i64>(message.BodySize, ArrowAlignment); // body
        }
        return size;
    }

    void WritePayload(TMutableRef payloadRef)
    {
        YT_LOG_DEBUG("Started writing payload (Size: %v)",
            payloadRef.Size());
        char* current = payloadRef.Begin();
        for (const auto& message : Messages_) {
            // Continuation indicator
            *reinterpret_cast<ui32*>(current) = 0xFFFFFFFF;
            current += sizeof(ui32);

            // Metadata size
            *reinterpret_cast<ui32*>(current) = AlignUp<i64>(message.FlatbufBuilder.GetSize(), ArrowAlignment);
            current += sizeof(ui32);

            // Metadata message
            std::copy(
                message.FlatbufBuilder.GetBufferPointer(),
                message.FlatbufBuilder.GetBufferPointer() + message.FlatbufBuilder.GetSize(),
                current);
            current += AlignUp<i64>(message.FlatbufBuilder.GetSize(), ArrowAlignment);

            // Body
            if (message.BodyWriter) {
                message.BodyWriter(TMutableRef(current, current + message.BodySize));
                current += AlignUp<i64>(message.BodySize, ArrowAlignment);
            } else {
                YT_VERIFY(message.BodySize == 0);
            }
        }
        YT_VERIFY(current == payloadRef.End());
        YT_LOG_DEBUG("Finished writing payload");
    }

private:
    const TTableSchemaPtr Schema_;
    const TNameTablePtr NameTable_;
    const IUnversionedColumnarRowBatchPtr Batch_;

    std::vector<TTypedBatchColumn> TypedColumns_;

    struct TMessage
    {
        flatbuffers::FlatBufferBuilder FlatbufBuilder;
        i64 BodySize;
        TBodyWriter BodyWriter;
    };

    std::vector<TMessage> Messages_;

    void RegisterMessage(
        org::apache::arrow::flatbuf::MessageHeader type,
        flatbuffers::FlatBufferBuilder&& flatbufBuilder,
        i64 bodySize = 0,
        std::function<void(TMutableRef)> bodyWriter = nullptr)
    {
        YT_LOG_DEBUG("Message registered (Type: %v, MessageSize: %v, BodySize: %v)",
            org::apache::arrow::flatbuf::EnumNamesMessageHeader()[type],
            flatbufBuilder.GetSize(),
            bodySize);

        YT_VERIFY((bodySize % ArrowAlignment) == 0);
        Messages_.push_back(TMessage{
            std::move(flatbufBuilder),
            bodySize,
            std::move(bodyWriter)
        });
    }

    const TColumnSchema& GetColumnSchema(const TBatchColumn& column)
    {
        YT_VERIFY(column.Id >= 0);
        auto name = NameTable_->GetName(column.Id);
        return Schema_->GetColumn(name);
    }

    void PrepareColumns()
    {
        auto batchColumns = Batch_->MaterializeColumns();
        TypedColumns_.reserve(batchColumns.Size());
        for (const auto* column : batchColumns) {
            TypedColumns_.push_back(TTypedBatchColumn{
                column,
                GetColumnSchema(*column).LogicalType()
            });
        }
    }

    void PrepareSchema()
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;
        
        int dictionaryIdCounter = 0;
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fieldOffsets;
        
        for (const auto& typedColumn : TypedColumns_) {
            const auto& columnSchema = GetColumnSchema(*typedColumn.Column);
            
            auto nameOffset = SerializeString(&flatbufBuilder, columnSchema.Name());
            
            auto [typeType, typeOffset] = SerializeColumnType(&flatbufBuilder, columnSchema);
            
            auto dictionaryEncodingOffset = SerializeDictionaryEncoding(
                &flatbufBuilder,
                *typedColumn.Column,
                &dictionaryIdCounter);
            
            auto fieldOffset = org::apache::arrow::flatbuf::CreateField(
                flatbufBuilder,
                nameOffset,
                columnSchema.LogicalType()->IsNullable(),
                typeType,
                typeOffset,
                dictionaryEncodingOffset);
            
            fieldOffsets.push_back(fieldOffset);
        }

        auto fieldsOffset = flatbufBuilder.CreateVector(fieldOffsets);

        auto schemaOffset = org::apache::arrow::flatbuf::CreateSchema(
            flatbufBuilder,
            org::apache::arrow::flatbuf::Endianness_Little,
            fieldsOffset);
        
        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_Schema,
            schemaOffset.Union(),
            0);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_Schema,
            std::move(flatbufBuilder));
    }

    void PrepareDictionaryBatches()
    {
        int dictionaryIdCounter = 0;
        auto prepareDictionaryBatch = [&] (const TTypedBatchColumn& typedColumn, const TBatchColumn* dictionaryColumn) {
            PrepareDictionaryBatch(
                TTypedBatchColumn{dictionaryColumn, typedColumn.Type},
                dictionaryIdCounter++);
        };

        for (const auto& typedColumn : TypedColumns_) {
            if (typedColumn.Column->Dictionary) {
                YT_LOG_DEBUG("Adding dictionary batch for dictionary-encoded column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(typedColumn, typedColumn.Column->Dictionary->ValueColumn);
            } else if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column)) {
                YT_LOG_DEBUG("Adding dictionary batch for RLE- but not dictionary-encoded string-like column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(typedColumn, typedColumn.Column->Rle->ValueColumn);
            } else if (IsRleDictionaryEncodedColumn(*typedColumn.Column)) {
                YT_LOG_DEBUG("Adding dictionary batch for RLE- and dictionary-encoded column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(typedColumn, typedColumn.Column->Rle->ValueColumn->Dictionary->ValueColumn);
            }
        }
    }

    void PrepareDictionaryBatch(
        const TTypedBatchColumn& typedColumn,
        int dictionaryId)
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        auto [recordBatchOffset, bodySize, bodyWriter] = SerializeRecordBatch(
            &flatbufBuilder,
            typedColumn.Column->ValueCount,
            MakeRange({typedColumn}));

        auto dictionaryBatchOffset = org::apache::arrow::flatbuf::CreateDictionaryBatch(
            flatbufBuilder,
            dictionaryId,
            recordBatchOffset);

        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_DictionaryBatch,
            dictionaryBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_DictionaryBatch,
            std::move(flatbufBuilder),
            bodySize,
            std::move(bodyWriter));
    }

    void PrepareRecordBatch()
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        auto [recordBatchOffset, bodySize, bodyWriter] = SerializeRecordBatch(
            &flatbufBuilder,
            Batch_->GetRowCount(),
            TypedColumns_);

        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_RecordBatch,
            recordBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_RecordBatch,
            std::move(flatbufBuilder),
            bodySize,
            std::move(bodyWriter));
    }
};

} // namespace 

////////////////////////////////////////////////////////////////////////////////

class TArrowRowStreamEncoder
    : public IRowStreamEncoder
{
public:
    TArrowRowStreamEncoder(
        TTableSchemaPtr schema,
        TNameTablePtr nameTable)
        : Schema_(std::move(schema))
        , NameTable_(std::move(nameTable))
        , FallbackEncoder_(CreateWireRowStreamEncoder(NameTable_))
    {
        YT_LOG_DEBUG("Row stream encoder created (Schema: %v)",
            *Schema_);
    }
    
    virtual TSharedRef Encode(
        const IUnversionedRowBatchPtr& batch,
        const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics) override
    {
        auto columnarBatch = batch->TryAsColumnar();
        if (!columnarBatch) {
            YT_LOG_DEBUG("Encoding non-columnar batch; running fallback");
            return FallbackEncoder_->Encode(batch, statistics);
        }

        YT_LOG_DEBUG("Encoding columnar batch (RowCount: %v)",
            batch->GetRowCount());

        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        descriptor.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        descriptor.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);
        descriptor.set_rowset_format(NApi::NRpcProxy::NProto::RF_ARROW);

        TArrowRowStreamBlockEncoder blockEncoder(Schema_, NameTable_, std::move(columnarBatch));

        auto [block, payloadRef] = SerializeRowStreamBlockEnvelope(
            blockEncoder.GetPayloadSize(),
            descriptor,
            statistics);

        blockEncoder.WritePayload(payloadRef);

        return block;
    }

private:
    const TTableSchemaPtr Schema_;
    const TNameTablePtr NameTable_;

    const IRowStreamEncoderPtr FallbackEncoder_;
};

IRowStreamEncoderPtr CreateArrowRowStreamEncoder(
    TTableSchemaPtr schema,
    TNameTablePtr nameTable)
{
    return New<TArrowRowStreamEncoder>(
        std::move(schema),
        std::move(nameTable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow

