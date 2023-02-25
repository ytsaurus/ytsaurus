#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types_proto3.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/yson/node/node_io.h>

using namespace NYT;
using namespace NYT::NTesting;

void InferringNodeMapperPrepareOperation(
    const IOperationPreparationContext& context,
    TJobOperationPreparer& builder)
{
    for (int i = 0; i < context.GetInputCount(); ++i) {
        auto schema = context.GetInputSchema(i);
        schema.AddColumn(TColumnSchema().Name("even").Type(EValueType::VT_INT64));
        builder.OutputSchema(2 * i, schema);
        schema.MutableColumns().back() = TColumnSchema().Name("odd").Type(EValueType::VT_INT64);
        builder.OutputSchema(2 * i + 1, schema);
    }
}

// This mapper maps `n` input tables to `2n` output tables.
// First `n` tables are duplicated into outputs `0,2,...,2n-2` and `1,3,...,2n-1`,
// adding "int64" columns "even" and "odd" to schemas correspondingly.
class TInferringNodeMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto even = reader->MoveRow();
            auto odd = even;
            even["even"] = 100;
            odd["odd"] = 101;
            writer->AddRow(even, 2 * reader->GetTableIndex());
            writer->AddRow(odd, 2 * reader->GetTableIndex() + 1);
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        InferringNodeMapperPrepareOperation(context, builder);
    }
};
REGISTER_MAPPER(TInferringNodeMapper);

// This mapper maps `n` input tables to `2n` output tables.
// First `n` tables are duplicated into outputs `0,2,...,2n-2` and `1,3,...,2n-1`,
// adding "int64" columns "even" and "odd" to schemas correspondingly.
class TInferringRawMapper : public IRawJob
{
public:
    void Do(const TRawJobContext& context) override
    {
        TIFStream input(context.GetInputFile());
        TVector<THolder<TOFStream>> outputs;
        for (const auto& output : context.GetOutputFileList()) {
            outputs.push_back(MakeHolder<TOFStream>(output));
        }
        auto rows = NodeFromYsonStream(&input, ::NYson::EYsonType::ListFragment);
        int tableIndex = 0;
        for (const auto& row : rows.AsList()) {
            if (row.IsNull()) {
                auto tableIndexNode = row.GetAttributes()["table_index"];
                if (!tableIndexNode.IsUndefined()) {
                    tableIndex = tableIndexNode.AsInt64();
                }
                continue;
            }
            auto even = row;
            auto odd = even;
            even["even"] = 100;
            odd["odd"] = 101;
            NodeToYsonStream(even, outputs[2 * tableIndex].Get());
            NodeToYsonStream(odd, outputs[2 * tableIndex + 1].Get());
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        InferringNodeMapperPrepareOperation(context, builder);
    }
};
REGISTER_RAW_JOB(TInferringRawMapper);

// This mapper sends all the input rows into the 0-th output stream.
// Moreover, a row from i-th table is sent to (i + 1)-th output stream.
// Schema for 0-th table is the result of concatenation of all the input schemas,
// other output schemas are copied as-is.
class TInferringMapperForMapReduce : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow(), 0);
            writer->AddRow(reader->MoveRow(), 1 + reader->GetTableIndex());
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputCount() + 1, context.GetOutputCount());

        TTableSchema bigSchema;
        for (int i = 0; i < context.GetInputCount(); ++i) {
            auto schema = context.GetInputSchema(i);
            UNIT_ASSERT(!schema.Empty());
            builder.OutputSchema(i + 1, schema);
            for (const auto& column : schema.Columns()) {
                bigSchema.AddColumn(column);
            }
        }
        builder.OutputSchema(0, bigSchema);
    }
};
REGISTER_MAPPER(TInferringMapperForMapReduce);

// This reduce combiner retains only the columns from `ColumnsToRetain_`.
class TInferringReduceCombiner : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    TInferringReduceCombiner() = default;

    TInferringReduceCombiner(THashSet<TString> columnsToRetain)
        : ColumnsToRetain_(std::move(columnsToRetain))
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            auto row = reader->MoveRow();
            TNode out = TNode::CreateMap();
            for (const auto& toRetain : ColumnsToRetain_) {
                if (row.HasKey(toRetain)) {
                    out[toRetain] = std::move(row[toRetain]);
                }
            }
            writer->AddRow(out);
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputCount(), 1);
        UNIT_ASSERT(!context.GetInputSchema(0).Empty());

        TTableSchema result;
        for (const auto& column : context.GetInputSchema(0).Columns()) {
            if (ColumnsToRetain_.contains(column.Name())) {
                result.AddColumn(column);
            }
        }
        builder.OutputSchema(0, result);
    }

    Y_SAVELOAD_JOB(ColumnsToRetain_);

private:
    THashSet<TString> ColumnsToRetain_;
};
REGISTER_REDUCER(TInferringReduceCombiner);

// The reducer just outputs the passed rows. Schema is copied as-is.
class TInferringIdReducer : public TIdReducer
{
public:
    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputCount(), 1);
        UNIT_ASSERT(!context.GetInputSchema(0).Empty());
        builder.OutputSchema(0, context.GetInputSchema(0));
    }
};
REGISTER_REDUCER(TInferringIdReducer);

// This mapper infers one additional column.
template<class TBase>
class TInferringMapper : public TBase
{
public:
    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& builder) const override
    {
        UNIT_ASSERT_VALUES_EQUAL(context.GetInputCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetOutputCount(), 1);

        auto schema = context.GetInputSchema(0);
        UNIT_ASSERT(!schema.Empty());

        schema.AddColumn("extra", EValueType::VT_DOUBLE);
        builder.OutputSchema(0, schema);
    }
};
REGISTER_MAPPER(TInferringMapper<TUrlRowIdMapper>);

////////////////////////////////////////////////////////////////////////////////

class TGrepperOld
    : public IMapper<TTableReader<TGrepperRecord>, TTableWriter<TGrepperRecord>>
{
public:
    TGrepperOld() = default;
    TGrepperOld(TString pattern)
        : Pattern_(pattern)
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (const auto& cursor : *reader) {
            auto row = cursor.GetRow();
            if (row.GetKey() == Pattern_) {
                writer->AddRow(row);
            }
        }
    }

    Y_SAVELOAD_JOB(Pattern_);

private:
    TString Pattern_;
};
REGISTER_MAPPER(TGrepperOld);

class TGrepper
    : public IMapper<TTableReader<TGrepperRecord>, TTableWriter<TGrepperRecord>>
{
public:
    TGrepper() = default;
    TGrepper(TString column, TString pattern)
        : Pattern_(pattern)
        , Column_(column)
    { }

    void Do(TReader* reader, TWriter* writer) override
    {
        for (const auto& cursor : *reader) {
            auto row = cursor.GetRow();
            if (row.GetKey() == Pattern_) {
                writer->AddRow(row);
            }
        }
    }

    void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const override
    {
        preparer
            .InputColumnRenaming(0, {{Column_, "Key"}})
            .InputDescription<TGrepperRecord>(0)
            .OutputDescription<TGrepperRecord>(0, /* inferSchema */ false);
        auto schema = context.GetInputSchema(0);
        for (auto& column : schema.MutableColumns()) {
            if (column.Name() == Column_) {
                column.Name("Key");
            }
        }
        preparer.OutputSchema(0, schema);
    }

    Y_SAVELOAD_JOB(Pattern_, Column_);

private:
    TString Pattern_;
    TString Column_;
};
REGISTER_MAPPER(TGrepper);

class TReducerCount : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    explicit TReducerCount(TString keyColumn = {})
        : KeyColumn_(keyColumn)
    {}

    void Do(TReader* reader, TWriter* writer) override
    {
        TString key;
        i64 count = 0;
        for (const auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            if (key.empty()) {
                key = row[KeyColumn_].AsString();
            }
            ++count;
        }
        writer->AddRow(TNode()(KeyColumn_, key)("count", count));
    }

    void PrepareOperation(const IOperationPreparationContext& /* context */, TJobOperationPreparer& preparer) const override
    {
        preparer
            .OutputSchema(0, TTableSchema().AddColumn(KeyColumn_, VT_STRING).AddColumn("count", VT_INT64))
            .InputColumnFilter(0, {KeyColumn_});
    }

    Y_SAVELOAD_JOB(KeyColumn_);

private:
    TString KeyColumn_;
};
REGISTER_REDUCER(TReducerCount);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(PrepareOperation)
{
    void TestMap(bool raw)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto someSchema = TTableSchema().AddColumn("some_column", EValueType::VT_STRING);
        auto otherSchema = TTableSchema().AddColumn("other_column", EValueType::VT_INT64);

        TYPath someTable = workingDir + "/some_table";
        TYPath otherTable = workingDir + "/other_table";
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(someTable).Schema(someSchema));
            writer->AddRow(TNode()("some_column", "abc"));
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(otherTable).Schema(otherSchema));
            writer->AddRow(TNode()("other_column", 12));
            writer->Finish();
        }

        TVector<TYPath> outTables;
        for (int i = 0; i < 4; ++i) {
            outTables.push_back(workingDir + "/out_table_" + ToString(i));
        }
        if (raw) {
            auto spec = TRawMapOperationSpec()
                .Format(TFormat::YsonBinary())
                .AddInput(someTable)
                .AddInput(otherTable);
            for (const auto& outTable : outTables) {
                spec.AddOutput(outTable);
            }
            client->RawMap(spec, new TInferringRawMapper());
        } else {
            auto spec = TMapOperationSpec()
                .AddInput<TNode>(someTable)
                .AddInput<TNode>(otherTable);
            for (const auto& outTable : outTables) {
                spec.AddOutput<TNode>(outTable);
            }
            client->Map(spec, new TInferringNodeMapper());
        }

        TVector<TTableSchema> outSchemas;
        for (const auto& path : outTables) {
            outSchemas.emplace_back();
            Deserialize(outSchemas.back(), client->Get(path + "/@schema"));
        }

        for (int i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns().size(), 2);

            if (i < 2) {
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Name(), someSchema.Columns()[0].Name());
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Type(), someSchema.Columns()[0].Type());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Name(), otherSchema.Columns()[0].Name());
                UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[0].Type(), otherSchema.Columns()[0].Type());
            }

            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[1].Name(), (i % 2 == 0) ? "even" : "odd");
            UNIT_ASSERT_VALUES_EQUAL(outSchemas[i].Columns()[1].Type(), EValueType::VT_INT64);
        }
    }

    Y_UNIT_TEST(Map)
    {
        TestMap(/* raw */ false);
    }

    Y_UNIT_TEST(RawMap)
    {
        TestMap(/* raw */ true);
    }

    Y_UNIT_TEST(MapReduce)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto someSchema = TTableSchema()
            .AddColumn("some_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING)
            .AddColumn("some_column", EValueType::VT_STRING);
        auto otherSchema = TTableSchema()
            .AddColumn("other_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING)
            .AddColumn(TColumnSchema().Name("other_column").Type(EValueType::VT_INT64));

        TYPath someTable = workingDir + "/some_table";
        TYPath otherTable = workingDir + "/other_table";
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(someTable).Schema(someSchema));
            writer->AddRow(TNode()("some_column", "abc"));
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(otherTable).Schema(otherSchema));
            writer->AddRow(TNode()("other_column", 12));
            writer->Finish();
        }

        auto spec = TMapReduceOperationSpec()
            .AddInput<TNode>(someTable)
            .AddInput<TNode>(otherTable)
            .SortBy({"some_key", "other_key"})
            .MaxFailedJobCount(1)
            .ForceReduceCombiners(true);

        TVector<TYPath> mapperOutTables;
        for (int i = 0; i < 2; ++i) {
            mapperOutTables.push_back(workingDir + "/mapper_out_table_" + ToString(i));
            spec.AddMapOutput<TNode>(mapperOutTables.back());
        }

        TYPath outTable = workingDir + "/out_table";
        spec.AddOutput<TNode>(outTable);

        THashSet<TString> toRetain = {"some_key", "other_key", "other_column"};
        client->MapReduce(
            spec,
            new TInferringMapperForMapReduce(),
            new TInferringReduceCombiner(toRetain),
            new TInferringIdReducer());

        TVector<TTableSchema> mapperOutSchemas;
        for (const auto& path : mapperOutTables) {
            mapperOutSchemas.emplace_back();
            Deserialize(mapperOutSchemas.back(), client->Get(path + "/@schema"));
        }
        TTableSchema outSchema;
        Deserialize(outSchema, client->Get(outTable + "/@schema"));

        for (const auto& [index, expectedSchema] : TVector<std::pair<int, TTableSchema>>{{0, someSchema}, {1, otherSchema}}) {
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns().size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].Name(), expectedSchema.Columns()[0].Name());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].Type(), expectedSchema.Columns()[0].Type());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[0].SortOrder(), expectedSchema.Columns()[0].SortOrder());

            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[1].Name(), expectedSchema.Columns()[1].Name());
            UNIT_ASSERT_VALUES_EQUAL(mapperOutSchemas[index].Columns()[1].Type(), expectedSchema.Columns()[1].Type());
        }

        UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns().size(), 3);

        for (const auto& [index, expectedName, expectedType, expectedSortOrder] :
            TVector<std::tuple<int, TString, EValueType, TMaybe<ESortOrder>>>{
                {0, "some_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING},
                {1, "other_key", EValueType::VT_INT64, ESortOrder::SO_ASCENDING},
                {2, "other_column", EValueType::VT_INT64, Nothing()}})
        {
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].Name(), expectedName);
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].Type(), expectedType);
            UNIT_ASSERT_VALUES_EQUAL(outSchema.Columns()[index].SortOrder(), expectedSortOrder);
        }
    }

    template<typename TRow, class TIdMapper, class TInferringMapper>
    void TestPrecedenceOverInference()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TYPath input = workingDir + "/input";
        {
            auto path = TRichYPath(input);
            if constexpr (std::is_base_of_v<::google::protobuf::Message, TRow>) {
                path = WithSchema<TRow>(TRichYPath(input));
            }
            auto writer = client->CreateTableWriter<TRow>(path);
            TRow row;
            row.SetHost("ya.ru");
            row.SetPath("search");
            row.SetHttpCode(404);
            writer->AddRow(row);
            writer->Finish();
        }

        TYPath outputForInference = workingDir + "/output_for_inference";
        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(input)
                .template AddOutput<TRow>(outputForInference),
            new TIdMapper(),
            TOperationOptions()
                .InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>));

        {
            TTableSchema schema;
            Deserialize(schema, client->Get(outputForInference + "/@schema"));

            UNIT_ASSERT_VALUES_EQUAL(schema.Columns().size(), 3);
            for (const auto& [index, expectedName, expectedType] : TVector<std::tuple<int, TString, EValueType>>{
                {0, "Host", EValueType::VT_STRING},
                {1, "Path", EValueType::VT_STRING},
                {2, "HttpCode", EValueType::VT_INT32}})
            {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Name(), expectedName);
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Type(), expectedType);
            }
        }

        TYPath outputForBothInferences = workingDir + "/output_for_both_inferences";
        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(input)
                .template AddOutput<TRow>(outputForBothInferences),
            new TInferringMapper(),
            TOperationOptions()
                .InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>));

        TTableSchema schema;
        Deserialize(schema, client->Get(outputForBothInferences + "/@schema"));

        UNIT_ASSERT_VALUES_EQUAL(schema.Columns().size(), 4);
        for (const auto& [index, expectedName, expectedType] : TVector<std::tuple<int, TString, EValueType>>{
            {0, "Host", EValueType::VT_STRING},
            {1, "Path", EValueType::VT_STRING},
            {2, "HttpCode", EValueType::VT_INT32},
            {3, "extra", EValueType::VT_DOUBLE}})
        {
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Name(), expectedName);
            UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[index].Type(), expectedType);
        }
    }

    Y_UNIT_TEST(PrecedenceOverProtobufInference)
    {
        TestPrecedenceOverInference<TUrlRow, TUrlRowIdMapper, TInferringMapper<TUrlRowIdMapper>>();
    }

    Y_UNIT_TEST(JobPreparerOldWay)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");

        TVector<TNode> data = {
            TNode()("keyColumn", "we want it")("value", ":)"),
            TNode()("keyColumn", "not interested")("value", ":("),
            TNode()("keyColumn", "we want it")("value", ":-)"),
            TNode()("keyColumn", ":-(")("value", ":-(")
        };
        TVector<TNode> expected = {
            TNode()("Key", "we want it")("value", ":)"),
            TNode()("Key", "we want it")("value", ":-)"),
        };
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable.Schema(
                TTableSchema().AddColumn("keyColumn", VT_STRING).AddColumn("value", VT_STRING)));
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        auto pattern = "we want it";
        client->Map(
            new TGrepperOld(pattern),
            TStructuredTablePath(inputTable.RenameColumns({{"keyColumn", "Key"}}), TGrepperRecord::descriptor()),
            TStructuredTablePath(outputTable.RenameColumns({{"keyColumn", "Key"}}), TGrepperRecord::descriptor()));

        auto reader = client->CreateTableReader<TNode>(outputTable);
        TVector<TNode> result;
        for (const auto& cursor : *reader) {
            result.push_back(cursor.GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expected, result);
    }

    Y_UNIT_TEST(JobPreparer)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");

        TVector<TNode> data = {
            TNode()("keyColumn", "we want it")("value", ":)"),
            TNode()("keyColumn", "not interested")("value", ":("),
            TNode()("keyColumn", "we want it")("value", ":-)"),
            TNode()("keyColumn", ":-(")("value", ":-(")
        };
        TVector<TNode> expected = {
            TNode()("Key", "we want it")("value", ":)"),
            TNode()("Key", "we want it")("value", ":-)"),
        };
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable.Schema(
                TTableSchema().AddColumn("keyColumn", VT_STRING).AddColumn("value", VT_STRING)));
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        auto keyColumn = "keyColumn";
        auto pattern = "we want it";
        client->Map(
            new TGrepper(keyColumn, pattern),
            inputTable,
            outputTable);

        auto reader = client->CreateTableReader<TNode>(outputTable);
        TVector<TNode> result;
        for (const auto& cursor : *reader) {
            result.push_back(cursor.GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expected, result);

        TTableSchema schema;
        Deserialize(schema, client->Get(outputTable.Path_ + "/@schema"));
        UNIT_ASSERT_EQUAL(schema, TTableSchema()
            .AddColumn("Key", EValueType::VT_STRING)
            .AddColumn("value", EValueType::VT_STRING));
    }

    Y_UNIT_TEST(ReducerCount)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto inputTable = TRichYPath(workingDir + "/input");
        auto outputTable = TRichYPath(workingDir + "/output");

        TVector<TNode> data = {
            TNode()("keyColumn", "we want it")("value", ":)"),
            TNode()("keyColumn", "we want it")("value", ":-)"),
        };
        TVector<TNode> expected = {
            TNode()("keyColumn", "we want it")("count", 2),
        };

        auto inputSchema = TTableSchema()
            .AddColumn("keyColumn", VT_STRING)
            .AddColumn("value", VT_STRING);
        {
            auto writer = client->CreateTableWriter<TNode>(inputTable.Schema(inputSchema));
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        client->MapReduce(
            new TIdMapper(),
            new TReducerCount("keyColumn"),
            inputTable,
            outputTable,
            "keyColumn");

        TTableSchema outputSchema;
        Deserialize(outputSchema, client->Get(outputTable.Path_ + "/@schema"));
        UNIT_ASSERT_EQUAL(outputSchema, TTableSchema()
            .AddColumn("keyColumn", EValueType::VT_STRING)
            .AddColumn("count", EValueType::VT_INT64));

        auto reader = client->CreateTableReader<TNode>(outputTable);
        TVector<TNode> result;
        for (const auto& cursor : *reader) {
            result.push_back(cursor.GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expected, result);
    }

} // Y_UNIT_TEST_SUITE(PrepareOperation)

Y_UNIT_TEST_SUITE(SchemaInference)
{

    template<typename TRow, class TIdMapper, class TIdReducer>
    void TestSchemaInference(bool setOperationOptions)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TOperationOptions options;
        if (setOperationOptions) {
            options.InferOutputSchema(std::is_base_of_v<::google::protobuf::Message, TRow>);
        } else {
            TConfig::Get()->InferTableSchema = std::is_base_of_v<::google::protobuf::Message, TRow>;
        }

        {
            auto writer = client->CreateTableWriter<TRow>(workingDir + "/input");
            TRow row;
            row.SetHost("build01-myt.yandex.net");
            row.SetPath("~/.virmc");
            row.SetHttpCode(3213);
            writer->AddRow(row);
            writer->Finish();
        }

        auto checkSchema = [] (TNode schemaNode) {
            TTableSchema schema;
            Deserialize(schema, schemaNode);
            UNIT_ASSERT(AreSchemasEqual(
                schema,
                TTableSchema()
                    .AddColumn(TColumnSchema().Name("Host").Type(EValueType::VT_STRING))
                    .AddColumn(TColumnSchema().Name("Path").Type(EValueType::VT_STRING))
                    .AddColumn(TColumnSchema().Name("HttpCode").Type(EValueType::VT_INT32))));
        };

        client->Map(
            TMapOperationSpec()
                .template AddInput<TRow>(workingDir + "/input")
                .template AddOutput<TRow>(workingDir + "/map_output"),
            new TIdMapper,
            options);

        checkSchema(client->Get(workingDir + "/map_output/@schema"));

        client->MapReduce(
            TMapReduceOperationSpec()
                .template AddInput<TRow>(workingDir + "/input")
                .template AddOutput<TRow>(workingDir + "/mapreduce_output")
                .ReduceBy("Host"),
            new TIdMapper,
            new TIdReducer,
            options);

        checkSchema(client->Get(workingDir + "/mapreduce_output/@schema"));
    }

    Y_UNIT_TEST(ProtobufSchemaInference_Config)
    {
        TestSchemaInference<TUrlRow, TUrlRowIdMapper, TUrlRowIdReducer>(false);
    }

    Y_UNIT_TEST(ProtobufSchemaInference_Options)
    {
        TestSchemaInference<TUrlRow, TUrlRowIdMapper, TUrlRowIdReducer>(true);
    }

} // Y_UNIT_TEST_SUITE(SchemaInference)
