#include "common_benchmarks.h"

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/format.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/formats/protobuf_parser.h>
#include <yt/yt/client/formats/protobuf_writer.h>
#include <yt/yt/client/formats/skiff_parser.h>
#include <yt/yt/client/formats/skiff_writer.h>
#include <yt/yt/client/formats/skiff_yson_converter.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/experiments/random_row/random_row.h>

#include <util/stream/null.h>
#include <util/stream/str.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <library/cpp/yt/memory/new.h>

#include <vector>
#include <memory>

namespace NYT::NTableClientBenchmark {

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NFormats;
using namespace NYT::NRandomRow;
using namespace NSkiff;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TSkiffSchema> SkiffSchemaFromLogicalType(const TLogicalTypePtr& logicalType);

static EWireType GetSkiffTypeForSimpleLogicalType(ESimpleLogicalValueType logicalType)
{
    const auto valueType = GetPhysicalType(logicalType);
    switch (valueType) {
        case EValueType::Int64:
            return EWireType::Int64;
        case EValueType::Uint64:
            return EWireType::Uint64;
        case EValueType::String:
            return EWireType::String32;
        case EValueType::Any:
            return EWireType::Yson32;
        case EValueType::Boolean:
            return EWireType::Boolean;
        case EValueType::Double:
            return EWireType::Double;
        case EValueType::Null:
            return EWireType::Nothing;
        case EValueType::Composite:
            // NB. GetPhysicalType never returns EValueType::Composite
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(valueType);
}

static std::vector<std::shared_ptr<TSkiffSchema>> SkiffSchemasFromStructFields(const std::vector<TStructField>& fields)
{
    std::vector<std::shared_ptr<TSkiffSchema>> fieldSchemas;
    for (const auto& field : fields) {
        fieldSchemas.push_back(SkiffSchemaFromLogicalType(field.Type)->SetName(field.Name));
    }
    return fieldSchemas;
}

static std::vector<std::shared_ptr<TSkiffSchema>> SkiffSchemasFromTupleElements(const std::vector<TLogicalTypePtr>& elements)
{
    std::vector<std::shared_ptr<TSkiffSchema>> elementSchemas;
    for (const auto& element : elements) {
        elementSchemas.push_back(SkiffSchemaFromLogicalType(element));
    }
    return elementSchemas;
}

static std::shared_ptr<TSkiffSchema> CreateVariantSchema(std::vector<std::shared_ptr<TSkiffSchema>> elements)
{
    if (elements.size() > 256) {
        return CreateVariant16Schema(std::move(elements));
    } else {
        return CreateVariant8Schema(std::move(elements));
    }
}

std::shared_ptr<TSkiffSchema> SkiffSchemaFromLogicalType(const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleTypeSchema(GetSkiffTypeForSimpleLogicalType(logicalType->AsSimpleTypeRef().GetElement()));
        case ELogicalMetatype::Optional:
            return CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                SkiffSchemaFromLogicalType(logicalType->AsOptionalTypeRef().GetElement())});
        case ELogicalMetatype::List:
            return CreateRepeatedVariant8Schema({SkiffSchemaFromLogicalType(logicalType->AsListTypeRef().GetElement())});
        case ELogicalMetatype::Struct:
            return CreateTupleSchema(SkiffSchemasFromStructFields(logicalType->AsStructTypeRef().GetFields()));
        case ELogicalMetatype::Tuple:
            return CreateTupleSchema(SkiffSchemasFromTupleElements(logicalType->AsTupleTypeRef().GetElements()));
        case ELogicalMetatype::VariantStruct:
            return CreateVariantSchema(SkiffSchemasFromStructFields(logicalType->AsVariantStructTypeRef().GetFields()));
        case ELogicalMetatype::VariantTuple:
            return CreateVariantSchema(SkiffSchemasFromTupleElements(logicalType->AsVariantTupleTypeRef().GetElements()));
        case ELogicalMetatype::Dict:
            return CreateRepeatedVariant8Schema({
                CreateTupleSchema({
                    SkiffSchemaFromLogicalType(logicalType->AsDictTypeRef().GetKey()),
                    SkiffSchemaFromLogicalType(logicalType->AsDictTypeRef().GetValue())
                })
            });
        case ELogicalMetatype::Tagged:
            return SkiffSchemaFromLogicalType(logicalType->AsTaggedTypeRef().GetElement());
        case ELogicalMetatype::Decimal:
            THROW_ERROR_EXCEPTION("Not supported yet");
    }
    YT_ABORT();
}

std::shared_ptr<TSkiffSchema> SkiffSchemaFromTableSchema(const TTableSchemaPtr& tableSchema)
{
    std::vector<std::shared_ptr<TSkiffSchema>> columnSchemas;
    for (const auto& column : tableSchema->Columns()) {
        columnSchemas.push_back(SkiffSchemaFromLogicalType(column.LogicalType())->SetName(column.Name()));
    }
    return CreateTupleSchema(std::move(columnSchemas));
}

////////////////////////////////////////////////////////////////////////////////

EProtobufType GetProtobufTypeForSimpleLogicalType(ESimpleLogicalValueType logicalType)
{
    switch (logicalType) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
            return EProtobufType::Int32;
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
            return EProtobufType::Uint32;
        case ESimpleLogicalValueType::Int64:
            return EProtobufType::Int64;
        case ESimpleLogicalValueType::Uint64:
            return EProtobufType::Uint64;
        case ESimpleLogicalValueType::Float:
        case ESimpleLogicalValueType::Double:
            return EProtobufType::Double;
        case ESimpleLogicalValueType::Boolean:
            return EProtobufType::Bool;
        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Json:
            return EProtobufType::String;
        case ESimpleLogicalValueType::Any:
            return EProtobufType::Any;
        default:
            THROW_ERROR_EXCEPTION("%Qv is not suppored",
                logicalType);
    }
}

TProtobufColumnConfigPtr ProtobufColumnConfigFromLogicalType(const TLogicalTypePtr& logicalType, int fieldNumber)
{
    TProtobufColumnConfigPtr result;
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            result = New<TProtobufColumnConfig>();
            result->ProtoType = GetProtobufTypeForSimpleLogicalType(logicalType->AsSimpleTypeRef().GetElement());
            result->FieldNumber = fieldNumber;
            result->Repeated = false;
            break;
        }
        case ELogicalMetatype::Optional: {
            if (logicalType->AsOptionalTypeRef().IsElementNullable()) {
                THROW_ERROR_EXCEPTION("Nested optionals are not supported");
            }
            result = ProtobufColumnConfigFromLogicalType(logicalType->AsOptionalTypeRef().GetElement(), fieldNumber);
            break;
        }
        case ELogicalMetatype::List: {
            auto elementType = logicalType->AsListTypeRef().GetElement();
            if (!(elementType->GetMetatype() == ELogicalMetatype::Simple
                || elementType->GetMetatype() == ELogicalMetatype::Struct))
            {
                THROW_ERROR_EXCEPTION("List element metatype can only be Simple or Struct");
            }
            result = ProtobufColumnConfigFromLogicalType(elementType, fieldNumber);
            result->Repeated = true;
            break;
        }
        case ELogicalMetatype::Struct: {
            result = New<TProtobufColumnConfig>();
            result->ProtoType = EProtobufType::StructuredMessage;
            result->FieldNumber = fieldNumber;
            result->Repeated = false;
            int innerFieldNumber = 1;
            for (const auto& field : logicalType->AsStructTypeRef().GetFields()) {
                auto fieldConfig = ProtobufColumnConfigFromLogicalType(field.Type, innerFieldNumber++);
                fieldConfig->Name = field.Name;
                result->Fields.push_back(std::move(fieldConfig));
            }
            break;
        }
        default: {
            THROW_ERROR_EXCEPTION("Unsupported metatype %Qlv", logicalType->GetMetatype());
        }
    }
    result->CustomPostprocess();
    return result;
}

TProtobufFormatConfigPtr ProtobufFormatConfigFromTableSchema(const TTableSchemaPtr& tableSchema)
{
    auto tableConfig = New<TProtobufTableConfig>();
    int columnNumber = 1;
    for (const auto& column : tableSchema->Columns()) {
        auto config = ProtobufColumnConfigFromLogicalType(column.LogicalType(), columnNumber++);
        config->Name = column.Name();
        tableConfig->Columns.push_back(std::move(config));
    }
    auto formatConfig = New<TProtobufFormatConfig>();
    formatConfig->Tables = {std::move(tableConfig)};
    return formatConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TNullValueConsumer
    : public IValueConsumer
{
public:
    explicit TNullValueConsumer(const TTableSchemaPtr& schema)
        : TableSchema_(schema)
        , NameTable_(TNameTable::FromSchema(*schema))
    { }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return TableSchema_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    { }

    void OnValue(const TUnversionedValue& /*value*/) override
    { }

    void OnEndRow() override
    { }

private:
    TTableSchemaPtr TableSchema_;
    TNameTablePtr NameTable_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffIOFactory
    : public IFormatIOFactory
{
public:
    explicit TSkiffIOFactory(const TTableSchemaPtr& schema)
        : TableSchema_(schema)
        , SkiffSchema_(SkiffSchemaFromTableSchema(schema))
        , NameTable_(TNameTable::FromSchema(*schema))
    { }

    ISchemalessFormatWriterPtr CreateWriter(const IAsyncOutputStreamPtr& writerStream) override
    {
        return CreateWriterForSkiff(
            {SkiffSchema_},
            NameTable_,
            {TableSchema_},
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

    std::unique_ptr<IParser> CreateParser(IValueConsumer* valueConsumer) override
    {
        return CreateParserForSkiff(SkiffSchema_, valueConsumer);
    }

private:
    TTableSchemaPtr TableSchema_;
    std::shared_ptr<TSkiffSchema> SkiffSchema_;
    TNameTablePtr NameTable_;
};

class TProtobufIOFactory
    : public IFormatIOFactory
{
public:
    explicit TProtobufIOFactory(const TTableSchemaPtr& schema)
        : TableSchema_(schema)
        , ProtobufConfig_(ProtobufFormatConfigFromTableSchema(schema))
        , NameTable_(TNameTable::FromSchema(*schema))
    { }

    ISchemalessFormatWriterPtr CreateWriter(const IAsyncOutputStreamPtr& writerStream) override
    {
        return CreateWriterForProtobuf(
            ProtobufConfig_,
            {TableSchema_},
            NameTable_,
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

    std::unique_ptr<IParser> CreateParser(IValueConsumer* valueConsumer) override
    {
        return CreateParserForProtobuf(valueConsumer, ProtobufConfig_, 0);
    }

private:
    TTableSchemaPtr TableSchema_;
    TProtobufFormatConfigPtr ProtobufConfig_;
    TNameTablePtr NameTable_;
};

class TYsonIOFactory
    : public IFormatIOFactory
{
public:
    explicit TYsonIOFactory(const TTableSchemaPtr& schema)
        : TableSchema_(schema)
        , NameTable_(TNameTable::FromSchema(*schema))
    {
        Deserialize(
            YsonFormat_,
            BuildYsonNodeFluently()
                .BeginAttributes()
                    .Item("format")
                    .Value("binary")
                .EndAttributes()
                .Value("yson"));
    }

    ISchemalessFormatWriterPtr CreateWriter(const IAsyncOutputStreamPtr& writerStream) override
    {
        return CreateStaticTableWriterForFormat(
            YsonFormat_,
            NameTable_,
            {TableSchema_},
            writerStream,
            true,
            New<TControlAttributesConfig>(),
            0);
    }

    std::unique_ptr<IParser> CreateParser(IValueConsumer* valueConsumer) override
    {
        return CreateParserForFormat(YsonFormat_, valueConsumer);
    }

private:
    TTableSchemaPtr TableSchema_;
    TNameTablePtr NameTable_;
    TFormat YsonFormat_;
};

std::unique_ptr<IFormatIOFactory> CreateFormatIOFactory(EBenchmarkedFormat format, const TTableSchemaPtr& schema)
{
    switch (format) {
        case EBenchmarkedFormat::Skiff:
            return std::make_unique<TSkiffIOFactory>(schema);
        case EBenchmarkedFormat::Protobuf:
            return std::make_unique<TProtobufIOFactory>(schema);
        case EBenchmarkedFormat::Yson:
            return std::make_unique<TYsonIOFactory>(schema);
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void FormatWriterBenchmark(
    benchmark::State& state,
    EBenchmarkedFormat format,
    const TTableSchemaPtr& tableSchema)
{
    auto gen = CreateRandomRowGenerator(tableSchema, 42);
    auto owningData = gen->GenerateRows(DatasetRowCount);
    std::vector<TUnversionedRow> data;
    for (const auto& row : owningData) {
        data.emplace_back(row);
    }

    auto nullOutputStream = TNullOutput();
    auto nullAsyncStream = CreateAsyncAdapter(&nullOutputStream);
    auto writer = CreateFormatIOFactory(format, tableSchema)->CreateWriter(nullAsyncStream);

    for (auto _ : state) {
        auto it = data.data();
        while (it != data.data() + data.size()) {
            auto next = std::min(it + WriterRowRangeSize, data.data() + data.size());
            TRange<TUnversionedRow> range(it, next);
            writer->Write(range);
            it = next;
        }
    }
    writer->Close().Get().ThrowOnError();
    nullAsyncStream->Close().Get().ThrowOnError();
}

void FormatReaderBenchmark(
    benchmark::State& state,
    EBenchmarkedFormat format,
    const TTableSchemaPtr& tableSchema)
{
    auto gen = CreateRandomRowGenerator(tableSchema, 42);
    auto owningData = gen->GenerateRows(DatasetRowCount);
    std::vector<TUnversionedRow> data;
    for (const auto& row : owningData) {
        data.emplace_back(row);
    }

    auto factory = CreateFormatIOFactory(format, tableSchema);

    TString out;
    auto outStream = TStringOutput(out);
    auto writerStream = CreateAsyncAdapter(&outStream);
    auto writer = factory->CreateWriter(writerStream);

    writer->Write(TRange<TUnversionedRow>(data.data(), data.size()));
    writer->Close().Get().ThrowOnError();
    writerStream->Close().Get().ThrowOnError();

    TNullValueConsumer devnullConsumer(tableSchema);
    auto parser = factory->CreateParser(&devnullConsumer);
    for (auto _ : state) {
        TStringBuf remainingBuffer = out;
        while (!remainingBuffer.Empty()) {
            parser->Read(remainingBuffer.NextTokAt(ParserReadBufferSize));
        }
    }
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTableClientBenchmark
