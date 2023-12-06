#include <yt/yt/core/test_framework/framework.h>

#include "row_helpers.h"

#include <yt/yt/library/formats/arrow_parser.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/validate_logical_type.h>
#include <yt/yt/library/formats/format.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT {

namespace {

using namespace NFormats;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

std::string GetEos()
{
    std::string eos;
    eos.assign(4, 0);
    return eos;
}

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("%Qlv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeOutputFromRecordBatch(const std::shared_ptr<arrow::RecordBatch>& recordBatch)
{
    auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto arrowWriter = arrow::ipc::MakeStreamWriter(outputStream, recordBatch->schema()).ValueOrDie();
    ThrowOnError(arrowWriter->WriteRecordBatch(*recordBatch));
    auto buffer = outputStream->Finish().ValueOrDie();
    return buffer->ToString();
}

std::string MakeIntegerArrow(const std::vector<int8_t>& data)
{
    arrow::Int8Builder builder;

    for (const auto& value : data) {
        ThrowOnError(builder.Append(value));
    }

    auto intArray = builder.Finish();

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("integer", arrow::int8())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*intArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeOptionalIntegerArrow()
{
    arrow::Int8Builder builder;

    ThrowOnError(builder.Append(1));
    ThrowOnError(builder.AppendNull());
    ThrowOnError(builder.AppendNull());

    auto data = builder.Finish();

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("opt", arrow::int8())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*data};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeBooleanArrow(const std::vector<bool>& data)
{
    arrow::BooleanBuilder builder;

    for (const auto& value : data) {
        ThrowOnError(builder.Append(value));
    }

    auto boolArray = builder.Finish();

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("bool", arrow::boolean())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*boolArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeIntAndStringArrow(const std::vector<int8_t>& data, const std::vector<std::string>& stringData)
{
    arrow::Int8Builder builder;

    for (const auto& value : data) {
        ThrowOnError(builder.Append(value));
    }
    auto intArray = builder.Finish();

    arrow::StringBuilder stringBuilder;

    for (const auto& value : stringData) {
       ThrowOnError(stringBuilder.Append(value));
    }

    auto stringArray = stringBuilder.Finish();

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("integer", arrow::int8()), arrow::field("string", arrow::binary())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*intArray, *stringArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeIntListArray(const std::vector<std::vector<int32_t>>& data)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ListBuilder> listBuilder;
    std::shared_ptr<arrow::Int32Builder> valueBuilder;

    valueBuilder = std::make_shared<arrow::Int32Builder>(pool);
    listBuilder = std::make_unique<arrow::ListBuilder>(pool, valueBuilder);

    for (const auto& list : data) {
        ThrowOnError(listBuilder->Append());
        for (const auto& value : list) {
            ThrowOnError(valueBuilder->Append(value));
        }
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("list", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    ThrowOnError(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeStringListArray(const std::vector<std::vector<std::string>>& data)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ListBuilder> listBuilder;
    std::shared_ptr<arrow::StringBuilder> valueBuilder;

    valueBuilder = std::make_shared<arrow::StringBuilder>(pool);
    listBuilder = std::make_unique<arrow::ListBuilder>(pool, valueBuilder);

    for (const auto& list : data) {
        ThrowOnError(listBuilder->Append());
        for (const auto& value : list) {
            ThrowOnError(valueBuilder->Append(value));
        }
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("list", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    ThrowOnError(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeMapArray(const std::vector<std::vector<int32_t>>& key, const std::vector<std::vector<int32_t>>& value)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::MapBuilder> map_builder;
    std::shared_ptr<arrow::Int32Builder> valueBuilder;
    std::shared_ptr<arrow::Int32Builder> keyBuilder;

    keyBuilder = std::make_shared<arrow::Int32Builder>(pool);
    valueBuilder = std::make_shared<arrow::Int32Builder>(pool);
    map_builder = std::make_unique<arrow::MapBuilder>(pool, keyBuilder, valueBuilder);

    for (int mapNumber = 0; mapNumber < std::ssize(key); mapNumber++) {
        ThrowOnError(map_builder->Append());
        for (int valueNumber = 0; valueNumber < std::ssize(key[mapNumber]); valueNumber++) {
            ThrowOnError(keyBuilder->Append(key[mapNumber][valueNumber]));
            ThrowOnError(valueBuilder->Append(value[mapNumber][valueNumber]));
        }
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("map", map_builder->type())});

    std::shared_ptr<arrow::Array> mapArray;
    ThrowOnError(map_builder->Finish(&mapArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {mapArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDictionaryArray()
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    arrow::DictionaryBuilder<arrow::Int32Type> dictionaryBuilder(pool);

    std::vector<int32_t> values = {1, 2, 1};
    for (const int32_t& value : values) {
        arrow::Status status = dictionaryBuilder.Append(value);
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("integer", dictionaryBuilder.type())});

    std::shared_ptr<arrow::Array> array;
    arrow::Status status = dictionaryBuilder.Finish(&array);
    std::vector<std::shared_ptr<arrow::Array>> columns = {array};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowParserTest, Simple)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("integer", EValueType::Int64)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntegerArrow({1, 2, 3});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
}

TEST(TArrowParserTest, Optional)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("opt", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeOptionalIntegerArrow();
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "opt")), 1);
    ASSERT_TRUE(IsNull(collectedRows.GetRowValue(1, "opt")));
    ASSERT_TRUE(IsNull(collectedRows.GetRowValue(2, "opt")));
}

TEST(TArrowParserTest, Dictionary)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("integer", EValueType::Int64)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeDictionaryArray();
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 1);
}

TEST(TArrowParserTest, Bool)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("bool", EValueType::Boolean)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeBooleanArrow({true, false, true});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(0, "bool")), true);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(1, "bool")), false);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(2, "bool")), true);
}

TEST(TArrowParserTest, String)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("integer", EValueType::Any),
        TColumnSchema("string", EValueType::String)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntAndStringArrow({1, 2, 3}, {"foo", "bar", "yt"});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "string")), "foo");

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "string")), "bar");

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(2, "string")), "yt");
}


TString ConvertToYsonTextStringStable(const INodePtr& node)
{
    TStringStream out;
    TYsonWriter writer(&out, EYsonFormat::Text);
    VisitTree(node, &writer, true, TAttributeFilter());
    writer.Flush();
    return out.Str();
}

TEST(TArrowParserTest, ListOfIntegers)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntListArray({{1, 2, 3}, {4, 5}});
    parser->Read(data);
    parser->Finish();

    auto intListNode = GetComposite(collectedRows.GetRowValue(0, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(intListNode), "[1;2;3;]");

    intListNode = GetComposite(collectedRows.GetRowValue(1, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(intListNode), "[4;5;]");
}

TEST(TArrowParserTest, ListOfStrings)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)))});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeStringListArray({{"foo", "bar"}, {"42", "universe"}});
    parser->Read(data);
    parser->Finish();

    auto stringListNode = GetComposite(collectedRows.GetRowValue(0, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(stringListNode), "[\"foo\";\"bar\";]");

    stringListNode = GetComposite(collectedRows.GetRowValue(1, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(stringListNode), "[\"42\";\"universe\";]");
}

TEST(TArrowParserTest, Map)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema(
            "map",
            DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64),
                SimpleLogicalType(ESimpleLogicalValueType::Uint64)))});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeMapArray({{1, 3}, {3}}, {{2, 2}, {2}});
    parser->Read(data);
    parser->Finish();

    auto mapNode = GetComposite(collectedRows.GetRowValue(0, "map"));
    ASSERT_EQ(ConvertToYsonTextStringStable(mapNode), "[[1;2;];[3;2;];]");

    mapNode = GetComposite(collectedRows.GetRowValue(1, "map"));
    ASSERT_EQ(ConvertToYsonTextStringStable(mapNode), "[[3;2;];]");
}

TEST(TArrowParserTest, SeveralIntArrays)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("integer", EValueType::Int64)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);
    auto data = Format("%v%v%v", MakeIntegerArrow({1, 2, 3}), GetEos(), MakeIntegerArrow({5, 6}));

    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 5u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(3, "integer")), 5);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(4, "integer")), 6);
}

TEST(TArrowParserTest, BlockingInput)
{
    TTableSchemaPtr tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("integer", EValueType::Int64)});

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntegerArrow({1, 2, 3});
    for (auto i : data) {
        std::string s(1, i);
        parser->Read(s);
    }
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3u);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
}

} // namespace

} // namespace NYT
