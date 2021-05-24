#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/all_types_proto3.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/clashing_enums.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

#include <mapreduce/yt/tests/native/ydl_lib/row.ydl.h>
#include <mapreduce/yt/tests/native/ydl_lib/all_types.ydl.h>

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/io/proto_table_reader.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <util/random/fast.h>

#include <type_traits>

using namespace NYT;
using namespace NYT::NTesting;

namespace NYdlRows = mapreduce::yt::tests::native::ydl_lib::row;
namespace NYdlAllTypes = mapreduce::yt::tests::native::ydl_lib::all_types;

static TString RandomBytes()
{
    static TReallyFastRng32 RNG(42);
    ui64 value = RNG.GenRand64();
    return TString((const char*)&value, sizeof(value));
}

template <typename TRow>
static NTi::TTypePtr GetRowType();

template <>
NTi::TTypePtr GetRowType<TUrlRow>()
{
    static auto type = NTi::Struct({
        {"Host", NTi::String()},
        {"Path", NTi::String()},
        {"HttpCode", NTi::Int32()},
    });
    return type;
}

template <>
NTi::TTypePtr GetRowType<TUrlRowWithColumnNames>()
{
    static auto type = NTi::Struct({
        {"Host_ColumnName", NTi::String()},
        {"Path_KeyColumnName", NTi::String()},
        {"HttpCode", NTi::Int32()},
    });
    return type;
}

template <>
void Out<TRowWithTypeOptions_Color>(IOutputStream& o, TRowWithTypeOptions_Color color)
{
    o << TRowWithTypeOptions::Color_Name(color);
}

Y_UNIT_TEST_SUITE(TableIo)
{

#define INSTANTIATE_NODE_READER_TESTS(test) \
    Y_UNIT_TEST(test ## _Yson_NonStrict) \
    { \
        TConfigSaverGuard configGuard; \
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson; \
        test(false); \
    } \
    Y_UNIT_TEST(test ## _Yson_Strict) \
    { \
        TConfigSaverGuard configGuard; \
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson; \
        test(true); \
    } \
    Y_UNIT_TEST(test ## _Skiff) \
    { \
        TConfigSaverGuard configGuard; \
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff; \
        test(true); \
    }

    TRichYPath CreatePath(const TYPath& workingDir, bool strictSchema)
    {
        TRichYPath path = workingDir + "/table";
        if (strictSchema) {
            path.Schema(TTableSchema().Strict(true)
                .AddColumn(TColumnSchema().Name("key1").Type(VT_STRING, true))
                .AddColumn(TColumnSchema().Name("key2").Type(VT_STRING))
                .AddColumn(TColumnSchema().Name("key3").Type(VT_STRING)));
        }
        return path;
    }

    void Simple(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = CreatePath(workingDir, strictSchema);
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(path);
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }
    INSTANTIATE_NODE_READER_TESTS(Simple)

    void NonEmptyColumns(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = CreatePath(workingDir, strictSchema);
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(path.Columns({"key1", "key3"}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(NonEmptyColumns_Yson)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
        NonEmptyColumns(false);
    }
    // TODO(levysotsky): Add Y_UNIT_TEST(NonEmptyColumns_Skiff) when client Skiff reader is ready.
    // See r3614168

    void EmptyColumns(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = CreatePath(workingDir, strictSchema);
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(path.Columns({}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode::CreateMap());
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(EmptyColumns_Yson)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
        EmptyColumns(false);
    }
    // TODO(levysotsky): Add Y_UNIT_TEST(EmptyColumns_Skiff) when client Skiff reader is ready.
    // See r3614168

    void MissingColumns()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = CreatePath(workingDir, /* strictSchema = */ true);
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key1", "value1")("key3", "value3"));
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(path);
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key2", TNode::CreateEntity())("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(MissingColumns_Yson)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Yson;
        MissingColumns();
    }

    Y_UNIT_TEST(MissingColumns_Skiff)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = ENodeReaderFormat::Skiff;
        MissingColumns();
    }

    void Move(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = CreatePath(workingDir, strictSchema);
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->AddRow(TNode()("key1", "value4")("key2", "value5")("key3", "value6"));
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(path);
        UNIT_ASSERT(reader->IsValid());

        UNIT_ASSERT_VALUES_EQUAL(reader->MoveRow(), TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
        UNIT_ASSERT_EXCEPTION(reader->MoveRow(), yexception);
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);

        reader->Next();

        {
            TNode row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row, TNode()("key1", "value4")("key2", "value5")("key3", "value6"));
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(), yexception);
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
            UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        }

        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }
    INSTANTIATE_NODE_READER_TESTS(Move)

    void ReadUncanonicalPath(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto path = TRichYPath(workingDir + "/table");
            if (strictSchema) {
                path.Schema(TTableSchema().Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_INT64).SortOrder(SO_ASCENDING)));
            } else {
                path.SortedBy("key");
            }
            auto writer = client->CreateTableWriter<TNode>(path);

            for (int i = 0; i < 100; ++i) {
                writer->AddRow(TNode()("key", i));
            }
            writer->Finish();
        }

        TRichYPath path = workingDir + "/table[#10:#20,30:40,#50:#60,70:80,#90,95]";

        TVector<i64> actual;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actual.push_back(row["key"].AsInt64());
        }

        const TVector<i64> expected = {
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            90,
            95,
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
    INSTANTIATE_NODE_READER_TESTS(ReadUncanonicalPath)

    void ReadMultipleRangesNode(bool strictSchema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto path = TRichYPath(workingDir + "/table");
            if (strictSchema) {
                path.Schema(TTableSchema().Strict(true)
                    .AddColumn(TColumnSchema().Name("key").Type(VT_INT64).SortOrder(SO_ASCENDING)));
            } else {
                path.SortedBy("key");
            }
            auto writer = client->CreateTableWriter<TNode>(path);

            for (int i = 0; i < 100; ++i) {
                writer->AddRow(TNode()("key", 1000 + i));
            }
            writer->Finish();
        }

        TRichYPath path(workingDir + "/table");
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().RowIndex(10))
            .UpperLimit(TReadLimit().RowIndex(20)));
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().Key(1030))
            .UpperLimit(TReadLimit().Key(1040)));
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().RowIndex(50))
            .UpperLimit(TReadLimit().RowIndex(60)));
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().Key(1070))
            .UpperLimit(TReadLimit().Key(1080)));
        path.AddRange(TReadRange()
            .Exact(TReadLimit().RowIndex(90)));
        path.AddRange(TReadRange()
            .Exact(TReadLimit().Key(1095)));

        TVector<i64> actualKeys;
        TVector<i64> actualRowIndices;
        TVector<ui32> actualRangeIndecies;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actualKeys.push_back(row["key"].AsInt64());
            actualRowIndices.push_back(reader->GetRowIndex());
            actualRangeIndecies.push_back(reader->GetRangeIndex());
        }

        const TVector<i64> expectedKeys = {
            1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019,
            1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039,
            1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059,
            1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079,
            1090,
            1095,
        };
        const TVector<i64> expectedRowIndices = {
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            90,
            95,
        };
        const TVector<ui32> expectedRangeIndicies = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
            4,
            5,
        };
        UNIT_ASSERT_VALUES_EQUAL(actualKeys, expectedKeys);
        UNIT_ASSERT_VALUES_EQUAL(actualRowIndices, expectedRowIndices);
        UNIT_ASSERT_VALUES_EQUAL(actualRangeIndecies, expectedRangeIndicies);
    }
    INSTANTIATE_NODE_READER_TESTS(ReadMultipleRangesNode)

#undef INSTANTIATE_NODE_READER_TESTS

    void TestNodeReader(ENodeReaderFormat format, bool strictSchema) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->NodeReaderFormat = format;

        auto path = workingDir + "/table";
        Y_DEFER {
            client->Remove(path, TRemoveOptions().Force(true));
        };

        auto row = TNode()
            ("int64", 1 - (1LL << 62))
            ("int16", 42 - (1 << 14))
            ("uint64", 1ULL << 63)
            ("uint16", 1U << 15)
            ("boolean", true)
            ("double", 1.4242e42)
            ("string", "Just a string");

        auto schema = TTableSchema().Strict(strictSchema);
        for (const auto& p : row.AsMap()) {
            EValueType type;
            Deserialize(type, p.first);
            schema.AddColumn(TColumnSchema().Name(p.first).Type(type));
        }
        {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(path).Schema(schema));
            writer->AddRow(row);
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(path);
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), row);
        UNIT_ASSERT_VALUES_EQUAL(reader->MoveRow(), row);
        UNIT_ASSERT_NO_EXCEPTION(reader->Next());
        UNIT_ASSERT(!reader->IsValid());
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
    }

    Y_UNIT_TEST(NodeReader_Skiff_Strict)
    {
        TestNodeReader(ENodeReaderFormat::Skiff, true);
    }
    Y_UNIT_TEST(NodeReader_Skiff_NonStrict)
    {
        // TODO(levysotsky): Assert an exception here when client Skiff reader is ready.
        // See r3614168
        TestNodeReader(ENodeReaderFormat::Skiff, false);
    }
    Y_UNIT_TEST(NodeReader_Auto_Strict)
    {
        TestNodeReader(ENodeReaderFormat::Auto, true);
    }
    Y_UNIT_TEST(NodeReader_Auto_NonStrict)
    {
        TestNodeReader(ENodeReaderFormat::Auto, false);
    }
    Y_UNIT_TEST(NodeReader_Yson_Strict)
    {
        TestNodeReader(ENodeReaderFormat::Yson, true);
    }
    Y_UNIT_TEST(NodeReader_Yson_NonStrict)
    {
        TestNodeReader(ENodeReaderFormat::Yson, false);
    }

    template<typename TRow>
    void TestTableReaders()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 302));
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TRow>(workingDir + "/table");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_NO_EXCEPTION(reader->GetRow());
        {
            TRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TRow row;
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
        }
        UNIT_ASSERT(reader->IsValid());

        reader->Next();
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        UNIT_ASSERT_NO_EXCEPTION(reader->GetRow());
        {
            TRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TRow row;
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
        }
        UNIT_ASSERT(reader->IsValid());

        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(Protobuf)
    {
        TestTableReaders<TUrlRow>();
    }

    Y_UNIT_TEST(Ydl)
    {
        TestTableReaders<NYdlRows::TUrlRow>();
    }

    Y_UNIT_TEST(UntypedProtobufWriter)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            TUrlRow row;
            row.SetHost("http://www.example.com");
            row.SetPath("/index.php");
            row.SetHttpCode(200);
            const Message* ptrWithoutType = &row;

            auto writer = client->CreateTableWriter(workingDir + "/urls", *TUrlRow::descriptor());
            writer->AddRow(*ptrWithoutType);
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TUrlRow>(workingDir + "/urls");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ProtobufVersions)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            const auto writer = client->CreateTableWriter<TRowVer1>(workingDir + "/ver1");
            TRowVer1 data;
            data.SetString_1("Ver1_String_1");
            data.SetUint32_2(0x12);
            writer->AddRow(data);
            writer->Finish();
        }

        //V1 as V2
        {
            const auto reader = client->CreateTableReader<TRowVer2>(workingDir + "/ver1");
            UNIT_ASSERT(reader->IsValid());
            {
                const auto& data = reader->GetRow();
                UNIT_ASSERT(data.HasString_1());
                UNIT_ASSERT_VALUES_EQUAL(data.GetString_1(), "Ver1_String_1");
                UNIT_ASSERT(data.HasUint32_2());
                UNIT_ASSERT_VALUES_EQUAL(data.GetUint32_2(), 0x12);
                UNIT_ASSERT(!data.HasFixed64_3());
                UNIT_ASSERT_VALUES_EQUAL(data.unknown_fields().field_count(), 0);
            }
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }

        {
            const auto writer = client->CreateTableWriter<TRowVer2>(workingDir + "/ver2");
            TRowVer2 data;
            data.SetString_1("Ver2_String_1");
            data.SetUint32_2(0x22);
            data.SetFixed64_3(0x23);
            writer->AddRow(data);
            writer->Finish();
        }

        //V2 as V1
        {
            const auto reader = client->CreateTableReader<TRowVer1>(workingDir + "/ver2");
            UNIT_ASSERT(reader->IsValid());
            {
                const auto& data = reader->GetRow();
                UNIT_ASSERT(data.HasString_1());
                UNIT_ASSERT_VALUES_EQUAL(data.GetString_1(), "Ver2_String_1");
                UNIT_ASSERT(data.HasUint32_2());
                UNIT_ASSERT_VALUES_EQUAL(data.GetUint32_2(), 0x22);
                //no unknown fields supported
                UNIT_ASSERT_VALUES_EQUAL(data.unknown_fields().field_count(), 0);
            }
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    struct TColumnNames
    {
        TString Host;
        TString Path;
        TString HttpCode;
    };

    template <typename TRow, typename TElement>
    void TestProtobufSerializationModes(
        EWrapperFieldFlag::Enum mode1,
        EWrapperFieldFlag::Enum mode2,
        const TColumnNames& columnNames = {"Host", "Path", "HttpCode"})
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto setType = [] (EWrapperFieldFlag::Enum mode, TColumnSchema& column) {
            switch (mode) {
                case EWrapperFieldFlag::SERIALIZATION_YT:
                    column.Type(GetRowType<TElement>());
                    return;
                case EWrapperFieldFlag::SERIALIZATION_PROTOBUF:
                    column.Type(EValueType::VT_STRING);
                    return;
                default:
                    Y_FAIL();
            }
        };

        auto schema = TTableSchema();
        auto column1 = TColumnSchema().Name("UrlRow_1");
        setType(mode1, column1);
        auto column2 = TColumnSchema().Name("UrlRow_2");
        setType(mode2, column2);
        schema.AddColumn(column1).AddColumn(column2);

        TVector<std::tuple<TString, TString, int>> firstValuesInRows{
            {"http://www.example.com", "/", 302},
            {"http://www.example.com", "/index.php", 200},
        };

        TVector<TVector<TString>> protobufSerializedValues;
        TVector<TVector<TNode>> nodeSerializedValues;
        {
            auto writer = client->CreateTableWriter<TRow>(
                TRichYPath(workingDir + "/table").Schema(schema));
            for (const auto& [host, path, code] : firstValuesInRows) {
                TRow row;
                auto& embedded1 = *row.MutableUrlRow_1();
                embedded1.SetHost(host);
                embedded1.SetPath(path);
                embedded1.SetHttpCode(code);
                auto& embedded2 = *row.MutableUrlRow_2();
                embedded2.SetHost(host);
                embedded2.SetPath(path);
                embedded2.SetHttpCode(code + 1);
                writer->AddRow(row);

                TString embedded1Serialized;
                embedded1.SerializeToString(&embedded1Serialized);
                TString embedded2Serialized;
                embedded2.SerializeToString(&embedded2Serialized);
                protobufSerializedValues.push_back(TVector<TString>{embedded1Serialized, embedded2Serialized});

                nodeSerializedValues.push_back(TVector<TNode>{
                    TNode()(columnNames.Host, host)(columnNames.Path, path)(columnNames.HttpCode, code),
                    TNode()(columnNames.Host, host)(columnNames.Path, path)(columnNames.HttpCode, code + 1),
                });
            }
            writer->Finish();
        }

        auto checkValue = [&] (const TNode& value, EWrapperFieldFlag::Enum mode, int rowIndex, int valueIndex) {
            switch (mode) {
                case EWrapperFieldFlag::SERIALIZATION_YT:
                    UNIT_ASSERT_VALUES_EQUAL(value, nodeSerializedValues[rowIndex][valueIndex]);
                    return;
                case EWrapperFieldFlag::SERIALIZATION_PROTOBUF:
                    UNIT_ASSERT_VALUES_EQUAL(value.GetType(), TNode::EType::String);
                    UNIT_ASSERT_VALUES_EQUAL(value.AsString(), protobufSerializedValues[rowIndex][valueIndex]);
                    return;
                default:
                    Y_FAIL();
            }
        };

        // Check that all the values were written correctly.
        {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT(reader->GetRow().HasKey("UrlRow_1"));
            UNIT_ASSERT(reader->GetRow().HasKey("UrlRow_2"));
            checkValue(reader->GetRow()["UrlRow_1"], mode1, 0, 0);
            checkValue(reader->GetRow()["UrlRow_2"], mode2, 0, 1);
            reader->Next();
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT(reader->GetRow().HasKey("UrlRow_1"));
            UNIT_ASSERT(reader->GetRow().HasKey("UrlRow_2"));
            checkValue(reader->GetRow()["UrlRow_1"], mode1, 1, 0);
            checkValue(reader->GetRow()["UrlRow_2"], mode2, 1, 1);
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }

        // Check that all the values can be read by protobuf reader.
        auto reader = client->CreateTableReader<TRow>(workingDir + "/table");
        for (const auto& [host, path, code] : firstValuesInRows) {
            UNIT_ASSERT(reader->IsValid());
            const auto& row = reader->GetRow();
            const auto& embedded1 = row.GetUrlRow_1();
            UNIT_ASSERT_VALUES_EQUAL(embedded1.GetHost(), host);
            UNIT_ASSERT_VALUES_EQUAL(embedded1.GetPath(), path);
            UNIT_ASSERT_VALUES_EQUAL(embedded1.GetHttpCode(), code);
            const auto& embedded2 = row.GetUrlRow_2();
            UNIT_ASSERT_VALUES_EQUAL(embedded2.GetHost(), host);
            UNIT_ASSERT_VALUES_EQUAL(embedded2.GetPath(), path);
            UNIT_ASSERT_VALUES_EQUAL(embedded2.GetHttpCode(), code + 1);
            reader->Next();
        }
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ProtobufSerializationMode_FieldOption)
    {
        TestProtobufSerializationModes<TRowFieldSerializationOption, TUrlRow>(
            EWrapperFieldFlag::SERIALIZATION_YT,
            EWrapperFieldFlag::SERIALIZATION_PROTOBUF);
    }

    Y_UNIT_TEST(ProtobufSerializationMode_MessageOption)
    {
        TestProtobufSerializationModes<TRowMessageSerializationOption, TUrlRow>(
            EWrapperFieldFlag::SERIALIZATION_YT,
            EWrapperFieldFlag::SERIALIZATION_YT);
    }

    Y_UNIT_TEST(ProtobufSerializationMode_MixedOptions)
    {
        TestProtobufSerializationModes<TRowMixedSerializationOptions, TUrlRow>(
            EWrapperFieldFlag::SERIALIZATION_YT,
            EWrapperFieldFlag::SERIALIZATION_PROTOBUF);
    }

    Y_UNIT_TEST(ProtobufSerializationMode_MixedOptions_ColumnNames)
    {
        TestProtobufSerializationModes<TRowMixedSerializationOptions_ColumnNames, TUrlRowWithColumnNames>(
            EWrapperFieldFlag::SERIALIZATION_YT,
            EWrapperFieldFlag::SERIALIZATION_PROTOBUF,
            {"Host_ColumnName", "Path_KeyColumnName", "HttpCode"});
    }

    Y_UNIT_TEST(ProtobufRepeatedSerialization)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto schema = TTableSchema()
            .AddColumn(TColumnSchema().Name("Ints").Type(NTi::List(NTi::Int64())))
            .AddColumn(TColumnSchema().Name("UrlRows").Type(NTi::List(GetRowType<TUrlRow>())))
            .AddColumn(TColumnSchema().Name("PackedInts").Type(NTi::List(NTi::Int64())));

        {
            auto writer = client->CreateTableWriter<TRowSerializedRepeatedFields>(
                TRichYPath(workingDir + "/table").Schema(schema));
            {
                TRowSerializedRepeatedFields row;
                row.AddInts(1);
                row.AddInts(2);
                row.AddInts(3);
                row.AddPackedInts(-1);
                row.AddPackedInts(-2);
                row.AddPackedInts(-3);
                auto& urlRow1 = *row.AddUrlRows();
                urlRow1.SetHost("http://www.example.com");
                urlRow1.SetPath("/");
                urlRow1.SetHttpCode(303);
                auto& urlRow2 = *row.AddUrlRows();
                urlRow2.SetHost("http://www.example.com");
                urlRow2.SetPath("/");
                urlRow2.SetHttpCode(307);
                writer->AddRow(row);
            }
            {
                TRowSerializedRepeatedFields row;
                row.AddInts(101);
                row.AddInts(201);
                row.AddInts(301);
                row.AddPackedInts(-101);
                row.AddPackedInts(-201);
                row.AddPackedInts(-301);
                auto& urlRow1 = *row.AddUrlRows();
                urlRow1.SetHost("http://www.example.com");
                urlRow1.SetPath("/index.php");
                urlRow1.SetHttpCode(200);
                auto& urlRow2 = *row.AddUrlRows();
                urlRow2.SetHost("http://www.example.com");
                urlRow2.SetPath("/index.php");
                urlRow2.SetHttpCode(201);
                writer->AddRow(row);
            }
            writer->Finish();
        }

        {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(
                reader->GetRow(),
                TNode()
                    ("Ints", TNode().Add(1).Add(2).Add(3))
                    ("PackedInts", TNode().Add(-1).Add(-2).Add(-3))
                    ("UrlRows", TNode()
                        .Add(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 303))
                        .Add(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 307))));
            reader->Next();
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(
                reader->GetRow(),
                TNode()
                    ("Ints", TNode().Add(101).Add(201).Add(301))
                    ("PackedInts", TNode().Add(-101).Add(-201).Add(-301))
                    ("UrlRows", TNode()
                        .Add(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200))
                        .Add(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 201))));
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }

        auto reader = client->CreateTableReader<TRowSerializedRepeatedFields>(workingDir + "/table");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            TVector<int> ints(row.GetInts().begin(), row.GetInts().end());
            UNIT_ASSERT_VALUES_EQUAL(ints, (TVector<int>{1, 2, 3}));
            TVector<int> packedInts(row.GetPackedInts().begin(), row.GetPackedInts().end());
            UNIT_ASSERT_VALUES_EQUAL(packedInts, (TVector<int>{-1, -2, -3}));
            UNIT_ASSERT_VALUES_EQUAL(row.UrlRowsSize(), 2);
            const auto& urlRow1 = row.GetUrlRows(0);
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetHttpCode(), 303);
            const auto& urlRow2 = row.GetUrlRows(1);
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetHttpCode(), 307);
        }
        reader->Next();
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            TVector<int> ints(row.GetInts().begin(), row.GetInts().end());
            UNIT_ASSERT_VALUES_EQUAL(ints, (TVector<int>{101, 201, 301}));
            TVector<int> packedInts(row.GetPackedInts().begin(), row.GetPackedInts().end());
            UNIT_ASSERT_VALUES_EQUAL(packedInts, (TVector<int>{-101, -201, -301}));
            UNIT_ASSERT_VALUES_EQUAL(row.UrlRowsSize(), 2);
            const auto& urlRow1 = row.GetUrlRows(0);
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(urlRow1.GetHttpCode(), 200);
            const auto& urlRow2 = row.GetUrlRows(1);
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(urlRow2.GetHttpCode(), 201);
        }
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ForbiddenRepeatedInProtobuf)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto schema = TTableSchema()
            .AddColumn(TColumnSchema().Name("Ints").Type(NTi::List(NTi::Int64())));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            client->CreateTableWriter<TBadProtobufSerializedRow>(
                TRichYPath(workingDir + "/table").Schema(schema)),
            TApiUsageError,
            "Repeated field NYT.NTesting.TBadProtobufSerializedRow.Ints must have flag SERIALIZATION_YT");
    }

    Y_UNIT_TEST(ProtobufWithTypeOption)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto schema = TTableSchema()
            .Strict(false)
            .AddColumn(TColumnSchema()
                .Name("ColorIntField").Type(EValueType::VT_INT64))
            .AddColumn(TColumnSchema()
                .Name("ColorStringField").Type(EValueType::VT_STRING))
            .AddColumn(TColumnSchema()
                .Name("AnyField").Type(EValueType::VT_ANY))
            .AddColumn(TColumnSchema()
                .Name("EmbeddedField").RawTypeV3(TNode()
                    ("type_name", "optional")
                    ("item", TNode()
                        ("type_name", "struct")
                        ("members", TNode()
                            .Add(TNode()
                                ("name", "ColorIntField")
                                ("type", "int64"))
                            .Add(TNode()
                                ("name", "ColorStringField")
                                ("type", "string"))
                            .Add(TNode()
                                ("name", "AnyField")
                                ("type", TNode()
                                    ("type_name", "optional")
                                    ("item", "yson")))))))
            .AddColumn(TColumnSchema()
                .Name("RepeatedEnumIntField").RawTypeV3(TNode()
                    ("type_name", "list")
                    ("item", "int64")))
            .AddColumn(TColumnSchema()
                .Name("UnknownSchematizedColumn").Type(EValueType::VT_BOOLEAN));

        auto node1 = TNode()("x", TNode()("y", 12));
        auto node2 = TNode()("key", "value");
        auto node3 = TNode().Add(1).Add("string").Add(true);
        auto node4 = TNode("hooray");

        {
            auto writer = client->CreateTableWriter<TRowWithTypeOptions>(
                TRichYPath(workingDir + "/table").Schema(schema));
            {
                TRowWithTypeOptions row;
                row.SetColorIntField(TRowWithTypeOptions::RED);
                row.SetColorStringField(TRowWithTypeOptions::BLUE);
                row.SetAnyField(NodeToYsonString(node1));
                row.SetOtherColumnsField(NodeToYsonString(
                    TNode()
                        ("UnknownSchematizedColumn", true)
                        ("UnknownUnschematizedColumn", 1234)));
                auto& embedded = *row.MutableEmbeddedField();
                embedded.SetColorIntField(TRowWithTypeOptions::WHITE);
                embedded.SetColorStringField(TRowWithTypeOptions::RED);
                embedded.SetAnyField(NodeToYsonString(node2));
                row.AddRepeatedEnumIntField(TRowWithTypeOptions::WHITE);
                row.AddRepeatedEnumIntField(TRowWithTypeOptions::BLUE);
                row.AddRepeatedEnumIntField(TRowWithTypeOptions::RED);
                writer->AddRow(row);
            }
            {
                TRowWithTypeOptions row;
                row.SetColorIntField(TRowWithTypeOptions::WHITE);
                row.SetColorStringField(TRowWithTypeOptions::RED);
                row.SetAnyField(NodeToYsonString(node3));
                row.SetOtherColumnsField(NodeToYsonString(
                    TNode()
                        ("UnknownSchematizedColumn", false)
                        ("UnknownUnschematizedColumn", "some-string")));
                auto& embedded = *row.MutableEmbeddedField();
                embedded.SetColorIntField(TRowWithTypeOptions::RED);
                embedded.SetColorStringField(TRowWithTypeOptions::WHITE);
                embedded.SetAnyField(NodeToYsonString(node4));
                row.AddRepeatedEnumIntField(TRowWithTypeOptions::BLUE);
                writer->AddRow(row);
            }
            writer->Finish();
        }

        {
            auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(
                reader->GetRow(),
                TNode()
                    ("ColorIntField", -1)
                    ("ColorStringField", "BLUE")
                    ("AnyField", node1)
                    ("UnknownSchematizedColumn", true)
                    ("UnknownUnschematizedColumn", 1234)
                    ("EmbeddedField", TNode()
                        ("ColorIntField", 0)
                        ("ColorStringField", "RED")
                        ("AnyField", node2))
                    ("RepeatedEnumIntField", TNode()
                        .Add(0)
                        .Add(1)
                        .Add(-1)));
            reader->Next();
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(
                reader->GetRow(),
                TNode()
                    ("ColorIntField", 0)
                    ("ColorStringField", "RED")
                    ("AnyField", node3)
                    ("UnknownSchematizedColumn", false)
                    ("UnknownUnschematizedColumn", "some-string")
                    ("EmbeddedField", TNode()
                        ("ColorIntField", -1)
                        ("ColorStringField", "WHITE")
                        ("AnyField", node4))
                    ("RepeatedEnumIntField", TNode().Add(1)));
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }

        auto reader = client->CreateTableReader<TRowWithTypeOptions>(workingDir + "/table");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetColorIntField(), TRowWithTypeOptions::RED);
            UNIT_ASSERT_VALUES_EQUAL(row.GetColorStringField(), TRowWithTypeOptions::BLUE);
            UNIT_ASSERT_VALUES_EQUAL(NodeFromYsonString(row.GetAnyField()), node1);
            auto otherColumns = NodeFromYsonString(row.GetOtherColumnsField());
            UNIT_ASSERT(otherColumns.HasKey("UnknownSchematizedColumn"));
            UNIT_ASSERT_VALUES_EQUAL(otherColumns["UnknownSchematizedColumn"], true);
            UNIT_ASSERT(otherColumns.HasKey("UnknownUnschematizedColumn"));
            UNIT_ASSERT_VALUES_EQUAL(otherColumns["UnknownUnschematizedColumn"], 1234);
            const auto& embedded = row.GetEmbeddedField();
            UNIT_ASSERT_VALUES_EQUAL(embedded.GetColorIntField(), TRowWithTypeOptions::WHITE);
            UNIT_ASSERT_VALUES_EQUAL(embedded.GetColorStringField(), TRowWithTypeOptions::RED);
            UNIT_ASSERT_VALUES_EQUAL(NodeFromYsonString(embedded.GetAnyField()), node2);

            TVector<TRowWithTypeOptions::Color> colors;
            for (auto intColor : row.GetRepeatedEnumIntField()) {
                colors.push_back(static_cast<TRowWithTypeOptions::Color>(intColor));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                colors,
                (TVector<TRowWithTypeOptions::Color>{
                    TRowWithTypeOptions::WHITE,
                    TRowWithTypeOptions::BLUE,
                    TRowWithTypeOptions::RED,
                }));
        }
        reader->Next();
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetColorIntField(), TRowWithTypeOptions::WHITE);
            UNIT_ASSERT_VALUES_EQUAL(row.GetColorStringField(), TRowWithTypeOptions::RED);
            UNIT_ASSERT_VALUES_EQUAL(NodeFromYsonString(row.GetAnyField()), node3);
            auto otherColumns = NodeFromYsonString(row.GetOtherColumnsField());
            UNIT_ASSERT(otherColumns.HasKey("UnknownSchematizedColumn"));
            UNIT_ASSERT_VALUES_EQUAL(otherColumns["UnknownSchematizedColumn"], false);
            UNIT_ASSERT(otherColumns.HasKey("UnknownUnschematizedColumn"));
            UNIT_ASSERT_VALUES_EQUAL(otherColumns["UnknownUnschematizedColumn"], "some-string");
            const auto& embedded = row.GetEmbeddedField();
            UNIT_ASSERT_VALUES_EQUAL(embedded.GetColorIntField(), TRowWithTypeOptions::RED);
            UNIT_ASSERT_VALUES_EQUAL(embedded.GetColorStringField(), TRowWithTypeOptions::WHITE);
            UNIT_ASSERT_VALUES_EQUAL(NodeFromYsonString(embedded.GetAnyField()), node4);

            TVector<TRowWithTypeOptions::Color> colors;
            for (auto intColor : row.GetRepeatedEnumIntField()) {
                colors.push_back(static_cast<TRowWithTypeOptions::Color>(intColor));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                colors,
                (TVector<TRowWithTypeOptions::Color>{TRowWithTypeOptions::BLUE}));
        }
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ErrorInTableWriter)
    {
        const TNode DATA = TString(1024, 'a');
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
        auto writeTable = [&] {
            for (int i = 0; i != 100000; ++i) {
                writer->AddRow(TNode()("foo", 0)("value", DATA));
            }
            writer->AddRow(TNode()("bar", "qux"));
            for (int i = 0; i != 100000; ++i) {
                writer->AddRow(TNode()("foo", 0)("value", DATA));
            }
            writer->Finish();
        };
        UNIT_ASSERT_EXCEPTION(writeTable(), TErrorResponse);
    }

    Y_UNIT_TEST(ErrorInFinish)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
        writer->AddRow(TNode()("bar", "qux"));
        UNIT_ASSERT_EXCEPTION(writer->Finish(), TErrorResponse);

        auto writeMore = [&] {
            writer->AddRow(TNode()("value", "a"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(writeMore(), TApiUsageError);
    }

    Y_UNIT_TEST(CantWriteAfterFinish)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
        writer->AddRow(TNode()("value", "foo"));
        writer->Finish();
        UNIT_ASSERT_EXCEPTION(writer->AddRow(TNode()("value", "a")), TApiUsageError);
    }

    Y_UNIT_TEST(HostsSlash)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        const TVector<TNode> tableData = {
            TNode()("foo", "bar"),
            TNode()("foo", "baz"),
        };

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/table");
            for (const auto& row : tableData) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        TConfig::Get()->Hosts = "/hosts";
        TConfig::Get()->UseHosts = true;
        TConfig::Get()->RetryCount = 1;
        TConfig::Get()->ReadRetryCount = 1;

        TVector<TNode> actual;
        auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
        for (; reader->IsValid(); reader->Next()) {
            actual.push_back(reader->GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(actual, tableData);
    }

    Y_UNIT_TEST(EmptyHosts)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(workingDir + "/table").SortedBy("key"));

            for (int i = 0; i < 10; ++i) {
                writer->AddRow(TNode()("key", i));
            }
            writer->Finish();
        }

        TConfig::Get()->Hosts = "hosts?role=ERROR";
        TConfig::Get()->UseHosts = true;
        TConfig::Get()->RetryCount = 1;
        TConfig::Get()->ReadRetryCount = 1;

        {
            auto tx = client->StartTransaction();
            UNIT_ASSERT_EXCEPTION(tx->CreateTableReader<NYT::TNode>(workingDir + "/table"), yexception);
        }
        {
            auto tx = client->StartTransaction();
            auto write = [=] {
                auto writer = tx->CreateTableWriter<NYT::TNode>(workingDir + "/table");
                writer->AddRow(TNode()("key", 0));
                writer->Finish();
            };
            UNIT_ASSERT_EXCEPTION(write(), yexception);
        }
    }

    Y_UNIT_TEST(ReadErrorInTrailers)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto writer = client->CreateTableWriter<NYT::TNode>(workingDir + "/table");
            for (int i = 0; i != 10000; ++i) {
                NYT::TNode node;
                node["key"] = RandomBytes();
                node["subkey"] = RandomBytes();
                node["value"] = RandomBytes();
                writer->AddRow(node);
            }
            NYT::TNode brokenNode;
            brokenNode["not_a_yamr_key"] = "ПЫЩ";
            writer->AddRow(brokenNode);
        }

        auto reader = client->CreateTableReader<NYT::TYaMRRow>(workingDir + "/table");

        // we expect first record to be read ok and error will come only later
        // in http trailer
        UNIT_ASSERT(reader->IsValid());

        auto readRemaining = [&]() {
            for (; reader->IsValid() ; reader->Next()) {
            }
        };
        UNIT_ASSERT_EXCEPTION(readRemaining(), NYT::TErrorResponse);
    }


    Y_UNIT_TEST(TableLockedForWriterLifetime)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto path = TRichYPath(workingDir + "/table").Append(false);
            auto writer = client->CreateTableWriter<TNode>(path);
            UNIT_ASSERT_EXCEPTION(client->CreateTableWriter<TNode>(path), TErrorResponse);
            writer->Finish();
        }
        {
            auto path = TRichYPath(workingDir + "/table").Append(true);
            auto firstWriter = client->CreateTableWriter<TNode>(path);
            UNIT_ASSERT_EXCEPTION(client->StartTransaction()->Lock(path.Path_, LM_EXCLUSIVE), TErrorResponse);
            // however we don't expect any exception here
            auto secondWriter = client->CreateTableWriter<TNode>(path);
            firstWriter->AddRow(TNode()("key", 100500));
            secondWriter->AddRow(TNode()("key", 2001000));
            firstWriter->Finish();
            secondWriter->Finish();
        }
    }

    size_t GetLockCount(IClientBasePtr client, const TString& path)
    {
        return client->Get(path + "/@lock_count").AsUint64();
    }

    Y_UNIT_TEST(OptionallyCreateChildTransactionForIO)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = TRichYPath(workingDir + "/table");
        {
            auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions().CreateTransaction(false));
            writer->AddRow(TNode()("key", 100500));
            UNIT_ASSERT_VALUES_EQUAL(GetLockCount(client, path.Path_), 0);
            writer->Finish();
        }
        {
            auto reader = client->CreateTableReader<TNode>(path, TTableReaderOptions().CreateTransaction(false));
            reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(GetLockCount(client, path.Path_), 0);
        }
        {
            // CreateTransaction default is true
            auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions());
            writer->AddRow(TNode()("key", 2001000));
            UNIT_ASSERT_VALUES_EQUAL(GetLockCount(client, path.Path_), 1);
            writer->Finish();
        }
        {
            // CreateTransaction default is true
            auto reader = client->CreateTableReader<TNode>(path, TTableReaderOptions());
            reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(GetLockCount(client, path.Path_), 1);
        }
        // Check that TableWriter never writes to global transaction if created under a local one
        client->Remove(path.Path_);
        {
            auto transaction = client->StartTransaction();
            auto writer = transaction->CreateTableWriter<TNode>(path, TTableWriterOptions().CreateTransaction(false));
            writer->AddRow(TNode()("key", 1234567));
            writer->Finish();
            auto transactionReader = transaction->CreateTableReader<TNode>(path);
            UNIT_ASSERT_VALUES_EQUAL(transactionReader->GetRow(), TNode()("key", 1234567));
            // For the client the table doesn't exist
            UNIT_ASSERT(!client->Exists(path.Path_));
            transaction->Commit();
            UNIT_ASSERT(client->Exists(path.Path_));
        }
    }

    Y_UNIT_TEST(ReaderTakesLockOnTableIdNotPath)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseAbortableResponse = true;

        auto firstPath = TRichYPath(workingDir + "/table1");
        auto secondPath = TRichYPath(workingDir + "/table2");
        int numRows = 4e6;
        {
            auto writer = client->CreateTableWriter<TNode>(firstPath);
            for (int i = 0; i < numRows; ++i) {
                writer->AddRow(TNode()("first_key", i));
            }
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(secondPath);
            for (int i = 0; i < numRows; ++i) {
                writer->AddRow(TNode()("second_key", i));
            }
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(firstPath);
        client->Move(secondPath.Path_, firstPath.Path_, TMoveOptions().Force(true));
        UNIT_ASSERT(TAbortableHttpResponse::AbortAll("/read_table") > 0);
        for (; reader->IsValid(); reader->Next()) {
            UNIT_ASSERT(reader->GetRow().AsMap().contains("first_key"));
        }
    }

    Y_UNIT_TEST(UnsuccessfulRetries)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 3;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(0);

        auto path = TRichYPath(workingDir + "/table");
        client->Create(path.Path_, ENodeType::NT_TABLE);

        try {
            auto outage = TAbortableHttpResponse::StartOutage("/write_table");
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key", "value"));
            writer->Finish();
            UNIT_FAIL("Retries must have been unsuccessful");
        } catch (const TAbortedForTestPurpose& e) {
            // It's OK
        }

        try {
            auto outage = TAbortableHttpResponse::StartOutage("/read_table");
            auto reader = client->CreateRawReader(path, TFormat::YsonBinary());
            reader->ReadAll();
            UNIT_FAIL("Retries must have been unsuccessful");
        } catch (const TAbortedForTestPurpose& e) {
            // It's OK
        }
    }

    Y_UNIT_TEST(SuccessfulRetries)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 4;

        auto path = TRichYPath(workingDir + "/table");
        {
            auto outage = TAbortableHttpResponse::StartOutage("/write_table", TConfig::Get()->RetryCount - 1);
            auto writer = client->CreateTableWriter<TNode>(path);
            writer->AddRow(TNode()("key", "value"));
            UNIT_ASSERT_NO_EXCEPTION(writer->Finish());
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage("/write_table", TConfig::Get()->RetryCount - 1);
            auto reader = client->CreateTableReader<TNode>(path);
            UNIT_ASSERT_VALUES_EQUAL(TNode()("key", "value"), reader->GetRow());
        }
    }

    Y_UNIT_TEST(TableReaderFromInputStream)
    {
        TString input = "{ key1 = [1; 2; 3; value0]; };  {key2 = { key21 = value1; key22 = value2 };}";
        TStringInput stream(input);
        TVector<TNode> expected = {
            TNode()("key1",
                TNode()
                .Add(1).Add(2).Add(3).Add("value0")),
            TNode()("key2",
                TNode()("key21", "value1")("key22", "value2"))
        };

        auto reader = CreateTableReader<TNode>(&stream);
        TVector<TNode> got;
        for (; reader->IsValid(); reader->Next()) {
            got.push_back(reader->GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(expected, got);
    }

    Y_UNIT_TEST(ReadingWritingProtobufAllTypesProto3)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto path = TRichYPath(workingDir + "/proto_table");
        TAllTypesMessageProto3 message;
        message.SetDoubleField(42.4242);
        message.SetFloatField(3.14159);
        message.SetInt64Field(-4200);
        // OmittedInt64Field is not set deliberately.
        message.SetUint64Field(4200);
        message.SetSint64Field(-4242);
        message.SetFixed64Field(432101234);
        message.SetSfixed64Field(41112222);
        message.SetInt32Field(-3124232);
        message.SetUint32Field(12321342);
        message.SetSint32Field(-42442);
        message.SetFixed32Field(2134242);
        message.SetSfixed32Field(422142);
        message.SetBoolField(true);
        message.SetStringField("42");
        message.SetBytesField("36 popugayev");
        message.SetEnumField(EEnumProto3::OneProto3);
        message.MutableMessageField()->SetKey("key");
        message.MutableMessageField()->SetValue("value");

        {
            auto writer = client->CreateTableWriter<TAllTypesMessageProto3>(path);
            writer->AddRow(message);
            writer->Finish();
        }
        {
            auto reader = client->CreateTableReader<TAllTypesMessageProto3>(path);
            UNIT_ASSERT(reader->IsValid());
            const auto& row = reader->GetRow();
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetDoubleField(), row.GetDoubleField());
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetFloatField(), row.GetFloatField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt64Field(), row.GetInt64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetOmittedInt64Field(), row.GetOmittedInt64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetOmittedInt64Field(), 0);
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint64Field(), row.GetUint64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSint64Field(), row.GetSint64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetFixed64Field(), row.GetFixed64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSfixed64Field(), row.GetSfixed64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt32Field(), row.GetInt32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint32Field(), row.GetUint32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSint32Field(), row.GetSint32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetFixed32Field(), row.GetFixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSfixed32Field(), row.GetSfixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBoolField(), row.GetBoolField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetStringField(), row.GetStringField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBytesField(), row.GetBytesField());
            UNIT_ASSERT_EQUAL(message.GetEnumField(), row.GetEnumField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetKey(), row.GetMessageField().GetKey());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetValue(), row.GetMessageField().GetValue());
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    Y_UNIT_TEST(ReadingWritingProtobufAllTypes)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto path = TRichYPath(workingDir + "/proto_table");
        TAllTypesMessage message;
        message.SetDoubleField(42.4242);
        message.SetFloatField(3.14159);
        message.SetInt64Field(-4200);
        message.SetUint64Field(4200);
        message.SetSint64Field(-4242);
        message.SetFixed64Field(432101234);
        message.SetSfixed64Field(41112222);
        message.SetInt32Field(-3124232);
        message.SetUint32Field(12321342);
        message.SetSint32Field(-42442);
        message.SetFixed32Field(2134242);
        message.SetSfixed32Field(422142);
        message.SetBoolField(true);
        message.SetStringField("42");
        message.SetBytesField("36 popugayev");
        message.SetEnumField(EEnum::One);
        message.MutableMessageField()->SetKey("key");
        message.MutableMessageField()->SetValue("value");

        {
            auto writer = client->CreateTableWriter<TAllTypesMessage>(path);
            writer->AddRow(message);
            writer->Finish();
        }
        {
            auto reader = client->CreateTableReader<TAllTypesMessage>(path);
            UNIT_ASSERT(reader->IsValid());
            const auto& row = reader->GetRow();
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetDoubleField(), row.GetDoubleField());
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetFloatField(), row.GetFloatField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt64Field(), row.GetInt64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint64Field(), row.GetUint64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSint64Field(), row.GetSint64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetFixed64Field(), row.GetFixed64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSfixed64Field(), row.GetSfixed64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt32Field(), row.GetInt32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint32Field(), row.GetUint32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSint32Field(), row.GetSint32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetFixed32Field(), row.GetFixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetSfixed32Field(), row.GetSfixed32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBoolField(), row.GetBoolField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetStringField(), row.GetStringField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBytesField(), row.GetBytesField());
            UNIT_ASSERT_EQUAL(message.GetEnumField(), row.GetEnumField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetKey(), row.GetMessageField().GetKey());
            UNIT_ASSERT_VALUES_EQUAL(message.GetMessageField().GetValue(), row.GetMessageField().GetValue());
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    Y_UNIT_TEST(ReadingWritingYdlAllTypes)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto path = TRichYPath(workingDir + "/ydl_table");
        NYdlAllTypes::TAllTypes message;

        message.SetDoubleField(42.4242);
        message.SetFloatField(3.14159);
        message.SetInt8Field(-10);
        message.SetInt16Field(-10000);
        message.SetInt32Field(-100000000);
        message.SetInt64Field(-100000000000);
        message.SetUint8Field(10);
        message.SetUint16Field(10000);
        message.SetUint32Field(100000000);
        message.SetUint64Field(100000000000);
        message.SetBoolField(true);
        message.SetStringField("42");
        message.SetDirectionField(NYdlAllTypes::Direction::north);

        {
            auto writer = client->CreateTableWriter<NYdlAllTypes::TAllTypes>(path);
            writer->AddRow(message);
            writer->Finish();
        }
        {
            auto reader = client->CreateTableReader<NYdlAllTypes::TAllTypes>(path);
            UNIT_ASSERT(reader->IsValid());
            const auto& row = reader->GetRow();
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetDoubleField(), row.GetDoubleField());
            UNIT_ASSERT_DOUBLES_EQUAL(1e-6, message.GetFloatField(), row.GetFloatField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt8Field(), row.GetInt8Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt16Field(), row.GetInt16Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt32Field(), row.GetInt32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetInt64Field(), row.GetInt64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint8Field(), row.GetUint8Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint16Field(), row.GetUint16Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint32Field(), row.GetUint32Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetUint64Field(), row.GetUint64Field());
            UNIT_ASSERT_VALUES_EQUAL(message.GetBoolField(), row.GetBoolField());
            UNIT_ASSERT_VALUES_EQUAL(message.GetStringField(), row.GetStringField());
            UNIT_ASSERT_EQUAL(message.GetDirectionField(), row.GetDirectionField());
            reader->Next();
            UNIT_ASSERT(!reader->IsValid());
        }
    }

    Y_UNIT_TEST(SimpleRetrylessWriter)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = TRichYPath(workingDir + "/table");
        const int numRows = 100;
        {
            auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions().SingleHttpRequest(true));
            for (int i = 0; i < numRows; ++i) {
                writer->AddRow(TNode()("key", i));
            }
        }
        auto reader = client->CreateTableReader<TNode>(path);
        int counter = 0;
        for (; reader->IsValid(); reader->Next()) {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key", counter));
            ++counter;
        }
        UNIT_ASSERT_VALUES_EQUAL(counter, numRows);
    }

    void TestCompressionCodec(EEncoding encoding)
    {
        TConfigSaverGuard configGuard;

        TConfig::Get()->ContentEncoding = encoding;
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = workingDir + "/table";

        const TVector<TNode> expectedData = {
            TNode()("foo", "bar"),
            TNode()("foo", "baz"),
        };

        {
            auto writer = client->CreateTableWriter<TNode>(path);
            for (const auto& row : expectedData) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(path);
        TVector<TNode> actual;
        for (; reader->IsValid(); reader->Next()) {
            actual.push_back(reader->GetRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(actual, expectedData);
    }

    Y_UNIT_TEST(CompressionCodecIdentity)
    {
        TestCompressionCodec(E_IDENTITY);
    }

    Y_UNIT_TEST(CompressionCodecGzip)
    {
        TestCompressionCodec(E_GZIP);
    }

    Y_UNIT_TEST(CompressionCodecBrotli)
    {
        TestCompressionCodec(E_BROTLI);
    }

    Y_UNIT_TEST(CompressionCodecZLz4)
    {
        TestCompressionCodec(E_Z_LZ4);
    }

    Y_UNIT_TEST(AbortWriter)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const int numRows = 2117;
        for (auto singleRequest : {true, false}) {
            auto path = TRichYPath(workingDir + "/table" + ToString(singleRequest));
            {
                auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions().SingleHttpRequest(singleRequest));
                for (int i = 0; i < numRows; ++i) {
                    writer->AddRow(TNode()("kluch", i));
                }
                writer->Abort();
            }
            UNIT_ASSERT(client->Get(path.Path_ + "/@row_count").AsInt64() < numRows); // Not everything was flushed
        }
    }

    void TryWriteTable(TTableWriterOptions options)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/file", options);
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }

    Y_UNIT_TEST(InvalidWriterOptionsFail)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteTable(TTableWriterOptions().WriterOptions(
                TWriterOptions()
                    .UploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/upload_replication_factor");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteTable(TTableWriterOptions().WriterOptions(
                TWriterOptions()
                    .MinUploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/min_upload_replication_factor");
    }

    template<typename TRow>
    void WriteAutoFlush()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TRow>(workingDir + "/table", TTableWriterOptions().CreateTransaction(false));
        UNIT_ASSERT_VALUES_EQUAL(client->Get(workingDir + "/table/@row_count").AsInt64(), 0);
        TRow row;
        for (size_t i = 0; i != 128; ++i) {
            row.SetHost(TString(1024 * 1024, 'a'));
            writer->AddRow(row);
        }

        UNIT_ASSERT(client->Get(workingDir + "/table/@row_count").AsInt64() > 0);
    }

    Y_UNIT_TEST(ProtobufWriteAutoflush)
    {
        WriteAutoFlush<TUrlRow>();
    }

    Y_UNIT_TEST(YdlWriteAutoflush)
    {
        WriteAutoFlush<NYdlRows::TUrlRow>();
    }

    Y_UNIT_TEST(TestFormatHint)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(workingDir + "/test_yson");
            writer->AddRow(TNode()("key", "foo")("value", TNode::CreateEntity()));
            writer->Finish();
        }

        {
            auto reader = client->CreateTableReader<TNode>(
                workingDir + "/test_yson",
                TTableReaderOptions()
                .FormatHints(TFormatHints().SkipNullValuesForTNode(true)));

            TVector<TNode> result;
            for (; reader->IsValid(); reader->Next()) {
                result.push_back(reader->GetRow());
            }

            const TVector<TNode> expected = {
                TNode()("key", "foo"),
            };
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST(TestComplexTypeMode)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto tablePath = workingDir + "/table";

        const auto structType = NTi::Struct({
            {"foo", NTi::String()},
            {"bar", NTi::Int64()},
        });
        const auto tableSchema = TTableSchema()
            .AddColumn(TColumnSchema().Name("value").Type(structType));

        const auto namedData = std::vector<TNode>{
            TNode::CreateMap({
                {
                    "value",
                    TNode::CreateMap({
                        {"foo", "foo-value"},
                        {"bar", 5},
                    })
                }
            }),
        };
        const auto positionalData = std::vector<TNode>{
            TNode::CreateMap({
                {"value", TNode::CreateList({"foo-value", 5})}
            })
        };

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(tablePath).Schema(tableSchema),
                TTableWriterOptions()
                    .FormatHints(TFormatHints().ComplexTypeMode(EComplexTypeMode::Positional))
            );
            for (const auto& row : positionalData) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        {
            auto actual = ReadTable(client, tablePath);
            UNIT_ASSERT_EQUAL(actual, namedData);
        }

        {
            auto reader = client->CreateTableReader<TNode>(
                tablePath,
                TTableReaderOptions()
                    .FormatHints(TFormatHints().ComplexTypeMode(EComplexTypeMode::Positional))
            );

            TVector<TNode> actual;
            for (const auto& cursor : *reader) {
                actual.push_back(cursor.GetRow());
            }

            UNIT_ASSERT_VALUES_EQUAL(actual, positionalData);
        }
    }

    // Checks we are able to read the whole table even if the connections
    // are aborted after every row. It emulates reading of a huge table
    // during which the server can drop the connections every now and then.
    Y_UNIT_TEST(OptimisticRetries)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 2;
        TConfig::Get()->RetryInterval = TDuration::MicroSeconds(10);

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto tablePath = TRichYPath(workingDir + "/table");
        int numRows = 20;
        {
            auto writer = client->CreateTableWriter<TNode>(tablePath);
            for (int i = 0; i < numRows; ++i) {
                writer->AddRow(TNode()("foo", TString(1 << 20, i + 'a')));
            }
            writer->Finish();
        }
        int abortedRequestCount = 0;
        {
            // We rely on the hope that one row will be stored in any case, so set the queue size to 1 byte.
            auto reader = client->CreateTableReader<TNode>(tablePath, TTableReaderOptions().SizeLimit(1));
            for (int i = 0; i < numRows; ++i) {
                UNIT_ASSERT(reader->IsValid());
                const auto& row = reader->GetRow();
                UNIT_ASSERT(AllOf(
                    row["foo"].AsString(),
                    [&i] (char c) {
                        return c == i + 'a';
                    }));

                abortedRequestCount += TAbortableHttpResponse::AbortAll("/read_table");

                reader->Next();
            }
            UNIT_ASSERT(!reader->IsValid());
        }
        // Check that there has been much more requests than RetryCount.
        UNIT_ASSERT_GE(abortedRequestCount, 10);
    }

    void TestProtobufSchemaInferring(bool setWriterOptions)
    {
        TTableWriterOptions options;
        TConfigSaverGuard configGuard;
        if (setWriterOptions) {
            options.InferSchema(true);
        } else {
            TConfig::Get()->InferTableSchema = true;
        }

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TRowVer2>(workingDir + "/table", options);
            TRowVer2 row;
            row.SetString_1("abc");
            row.SetUint32_2(40 + 2);
            row.SetFixed64_3(8888);
            writer->AddRow(row);
            writer->Finish();
        }

        TTableSchema actualSchema;
        Deserialize(actualSchema, client->Get(workingDir + "/table/@schema"));
        UNIT_ASSERT(AreSchemasEqual(
            actualSchema,
            TTableSchema()
                .AddColumn(TColumnSchema().Name("String_1").Type(EValueType::VT_STRING))
                .AddColumn(TColumnSchema().Name("Uint32_2").Type(EValueType::VT_UINT32))
                .AddColumn(TColumnSchema().Name("Fixed64_3").Type(EValueType::VT_UINT64))));
    }

    Y_UNIT_TEST(ProtobufSchemaInferring_Config)
    {
        TestProtobufSchemaInferring(false);
    }

    Y_UNIT_TEST(ProtobufSchemaInferring_Options)
    {
        TestProtobufSchemaInferring(true);
    }

    Y_UNIT_TEST(ProtobufWriteRead_Enum)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        TConfig::Get()->UseClientProtobuf = false;

        auto table = TRichYPath(workingDir + "/table");

        const TVector<EEnum> expected{EEnum::One, EEnum::Two, EEnum::Three, EEnum::MinusFortyTwo};
        {
            auto writer = client->CreateTableWriter<TAllTypesMessage>(table);

            for (const EEnum& enumField : expected) {
                TAllTypesMessage row;
                row.SetEnumField(enumField);
                row.SetEnumIntField(enumField);
                writer->AddRow(row);
            }

            writer->Finish();
        }

        TVector<EEnum> actual;
        {
            auto reader = client->CreateTableReader<TNode>(table);

            for (; reader->IsValid(); reader->Next()) {
                TAllTypesMessage row;
                ReadMessageFromNode(reader->GetRow(), &row);

                UNIT_ASSERT_EQUAL(row.GetEnumField(), row.GetEnumIntField());

                actual.push_back(row.GetEnumField());
            }
        }

        UNIT_ASSERT_EQUAL(expected.size(), actual.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            UNIT_ASSERT_EQUAL(expected[i], actual[i]);
        }
    }

    Y_UNIT_TEST(ProtoClashingEnums_YT_13714) {
        TTestFixture fixture;

        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto testTable = workingDir + "/table";

        TClashingEnumMessage originalRow;
        originalRow.SetEnum1(TClashingEnumMessage1::ClashingEnumValueOne);
        originalRow.SetEnum2(TClashingEnumMessage2::ClashingEnumValueTwo);
        originalRow.SetEnum3(ClashingEnumValueThree);
        {
            auto writer = client->CreateTableWriter<TClashingEnumMessage>(testTable);
            writer->AddRow(originalRow);
            writer->Finish();
        }

        TVector<TClashingEnumMessage> readValues;
        auto reader = client->CreateTableReader<TClashingEnumMessage>(testTable);
        for (const auto& cursor : *reader) {
            readValues.push_back(cursor.GetRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(readValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readValues[0].ShortUtf8DebugString(), originalRow.ShortUtf8DebugString());
    }
}

Y_UNIT_TEST_SUITE(BlobTableIo)
{
    Y_UNIT_TEST(Simple)
    {
        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1024 * 1024 * 4, 'b'),
            TString(1024 * 1024 * 4, 'c'),
            TString(1027, 'd'),
        };

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(workingDir + "/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            }

            {
                TNode row;
                row["filename"] = "myfile_small";
                row["part_index"] = 0;
                row["data"] = "small";
                writer->AddRow(row);
            }

            writer->Finish();
        }

        {
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_small"});
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), "small");
        }

        {
            TString expected;
            for (const auto& part : testDataParts) {
                expected += part;
            }
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_big"});
            UNIT_ASSERT_EQUAL(reader->ReadAll(), expected);
        }
    }

    Y_UNIT_TEST(WithOffset)
    {
        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1024 * 1024 * 4, 'b'),
            TString(1024 * 1024 * 4, 'c'),
            TString(1027, 'd'),
        };

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(workingDir + "/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            }

            {
                TNode row;
                row["filename"] = "myfile_small";
                row["part_index"] = 0;
                row["data"] = "small";
                writer->AddRow(row);
            }

            writer->Finish();
        }

        {
            i64 offset = 3;
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_small"},
                TBlobTableReaderOptions().Offset(offset));
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), /*sma*/"ll");
        }

        {
            i64 offset = 1e7;
            TString expected;
            for (const auto& part : testDataParts) {
                expected += part;
            }
            expected = expected.substr(offset);
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_big"},
                TBlobTableReaderOptions().Offset(offset));
            UNIT_ASSERT_EQUAL(reader->ReadAll(), expected);
        }
    }

    Y_UNIT_TEST(WithAbortableHttpResponse)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1024 * 1024 * 4, 'b'),
            TString(1024 * 1024 * 4, 'c'),
            TString(1027, 'd'),
        };

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(1);
        TConfig::Get()->ReadRetryCount = 5;

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(workingDir + "/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            }

            {
                TNode row;
                row["filename"] = "myfile_small";
                row["part_index"] = 0;
                row["data"] = "small";
                writer->AddRow(row);
            }

            writer->Finish();
        }

        {
            auto outage = TAbortableHttpResponse::StartOutage("/read_blob_table", TOutageOptions().LengthLimit(3));
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_small"});
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), "small");
        }

        TString expected;
        for (const auto& part : testDataParts) {
            expected += part;
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage(
                "/read_blob_table",
                TOutageOptions().LengthLimit(16 * 1024 * 1024));
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_big"});
            UNIT_ASSERT_EQUAL(reader->ReadAll(), expected);
        }
        {
            auto outage = TAbortableHttpResponse::StartOutage(
                "/read_blob_table",
                TOutageOptions().LengthLimit(1 * 1024 * 1024));
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_big"});
            UNIT_ASSERT_EQUAL(reader->ReadAll(), expected);
        }
    }

    Y_UNIT_TEST(WrongPartSize)
    {
        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1027, 'd'),
        };

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath(workingDir + "/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            }

            writer->Finish();
        }


        auto readFile = [&] (ui64 partSize) {
            auto reader = client->CreateBlobTableReader(workingDir + "/table", {"myfile_big"}, TBlobTableReaderOptions().PartSize(partSize));
            reader->ReadAll();
        };
        readFile(4 * 1024 * 1024); // no exception
        UNIT_ASSERT_EXCEPTION(readFile(100500), yexception);
    }

    Y_UNIT_TEST(TableReaderReadError_YT_12822) {
        class TRetryConfigProvider : public IRetryConfigProvider {
            public:
                TRetryConfig CreateRetryConfig() override {
                    return {TDuration::MilliSeconds(1)};
                }
        };
        TTestFixture fixture(TCreateClientOptions().RetryConfigProvider(MakeIntrusive<TRetryConfigProvider>()));

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(100);
        TConfig::Get()->RetryCount = Max<int>();
        TConfig::Get()->ReadRetryCount = Max<int>();

        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto testTable = workingDir + "/table";

        {
            auto writer = client->CreateTableWriter<TNode>(testTable);
            for (int i = 0; i != 1000; ++i) {
                writer->AddRow(TNode()("foo", "bar"));
            }
            writer->Finish();
        }

        auto outage = TAbortableHttpResponse::StartOutage("/read_table", TOutageOptions().LengthLimit(3));
        try {
            auto reader = client->CreateTableReader<TNode>(testTable);
            for (const auto cursor : *reader) {
            }
            UNIT_FAIL("Expected exception!!!!");
        } catch (yexception& ex) {
            // it's ok
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TableIoEnableTypeConversion)
{
    TTableSchema CreateSchemaForTypeConversion()
    {
        return TTableSchema()
            .Strict(true)
            .AddColumn(TColumnSchema().Name("String").Type(VT_STRING))
            .AddColumn(TColumnSchema().Name("Int64").Type(VT_INT64))
            .AddColumn(TColumnSchema().Name("Uint64").Type(VT_UINT64))
            .AddColumn(TColumnSchema().Name("Double").Type(VT_DOUBLE));
    }

    template <typename TRow>
    void CheckRowAfterWriting(
        const TFormatHints& hints,
        const TRow& writtenRow,
        const TRow& expectedRow)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TRow>(
            TRichYPath(workingDir + "/table").Schema(CreateSchemaForTypeConversion()),
            TTableWriterOptions().FormatHints(hints));
        writer->AddRow(writtenRow);
        writer->Finish();

        auto reader = client->CreateTableReader<TRow>(workingDir + "/table");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), expectedRow);
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    template <typename TRow>
    void WriteRowAndAssertException(
        const TFormatHints& hints,
        const TRow& row)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto writer = client->CreateTableWriter<TRow>(
            TRichYPath(workingDir + "/table").Schema(CreateSchemaForTypeConversion()),
            TTableWriterOptions().FormatHints(hints));
        writer->AddRow(row);
        UNIT_ASSERT_EXCEPTION(writer->Finish(), TErrorResponse);
    }

    Y_UNIT_TEST(AllToStringNode)
    {
        CheckRowAfterWriting(
            TFormatHints().EnableAllToStringConversion(true),
            TNode()("String", 123),
            TNode()("String", "123")("Int64", TNode::CreateEntity())("Uint64", TNode::CreateEntity())("Double", TNode::CreateEntity()));

        WriteRowAndAssertException(
            TFormatHints().EnableAllToStringConversion(false),
            TNode()("String", 123));
    }

    Y_UNIT_TEST(StringToAllNode)
    {
        CheckRowAfterWriting(
            TFormatHints().EnableStringToAllConversion(true),
            TNode()("Int64", "-123")("Uint64", "45")("Double", "3.14"),
            TNode()("String", TNode::CreateEntity())("Int64", -123)("Uint64", ui64(45))("Double", 3.14));

        WriteRowAndAssertException(
            TFormatHints().EnableStringToAllConversion(false),
            TNode()("Int64", "-123"));
    }

    Y_UNIT_TEST(IntegralTypeNode)
    {
        CheckRowAfterWriting(
            TFormatHints().EnableIntegralTypeConversion(true),
            TNode()("Int64", ui64(123))("Uint64", 45),
            TNode()("String", TNode::CreateEntity())("Int64", 123)("Uint64", ui64(45))("Double", TNode::CreateEntity()));

        WriteRowAndAssertException(
            TFormatHints().EnableIntegralTypeConversion(false),
            TNode()("Int64", 123u));
    }

    Y_UNIT_TEST(IntegralToDoubleNode)
    {
        CheckRowAfterWriting(
            TFormatHints().EnableIntegralToDoubleConversion(true),
            TNode()("Int64", 123)("Uint64", ui64(45))("Double", 3),
            TNode()("String", TNode::CreateEntity())("Int64", 123)("Uint64", ui64(45))("Double", 3.0));

        WriteRowAndAssertException(
            TFormatHints().EnableIntegralToDoubleConversion(false),
            TNode()("Double", 123));

    }

    Y_UNIT_TEST(AllNode)
    {
        CheckRowAfterWriting(
            TFormatHints().EnableTypeConversion(true),
            TNode()("String", 178)("Int64", "-123")("Uint64", "45")("Double", 3),
            TNode()("String", "178")("Int64", -123)("Uint64", ui64(45))("Double", 3.0));

        WriteRowAndAssertException(
            TFormatHints().EnableTypeConversion(false),
            TNode()("String", 178));
    }
}

///////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(StreamReaders)
{
    TString SerializeProto(const Message& row)
    {
        TString data;
        row.SerializeToString(&data);
        auto len = static_cast<ui32>(data.size());
        auto lenStr = TString(reinterpret_cast<char*>(&len), sizeof(len));
        auto dataWithLen = lenStr + data;
        return dataWithLen;
    }

    Y_UNIT_TEST(Protobuf)
    {
        TUrlRow row;
        row.SetHost("http://www.example.com");
        row.SetPath("/");
        row.SetHttpCode(302);

        auto data = SerializeProto(row);
        auto stream = TMemoryInput(data);
        auto reader = CreateTableReader<TUrlRow>(&stream);

        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ProtobufMultiTableHetero)
    {
        TUrlRow row1;
        row1.SetHost("http://www.example.com");
        row1.SetPath("/");
        row1.SetHttpCode(302);
        
        TRowVer1 row2;
        row2.SetString_1("String");
        row2.SetUint32_2(32);
        
        TUrlRow row3;
        row3.SetHost("http://www.example.com");
        row3.SetPath("/");
        row3.SetHttpCode(302);

        auto data = SerializeProto(row1) +
            "\xFF\xFF\xFF\xFF" + TString("\x01\x00\x00\x00", 4) + // Table index
            SerializeProto(row2) +
            SerializeProto(row2) +
            "\xFF\xFF\xFF\xFF" + TString("\x02\x00\x00\x00", 4) + // Table index
            SerializeProto(row3);

        auto stream = TMemoryInput(data);
        auto reader = CreateProtoMultiTableReader<TUrlRow, TRowVer1, TUrlRow>(&stream);

        static_assert(std::is_same_v<
            std::remove_reference_t<decltype(*reader)>,
            TTableReader<TProtoOneOf<TUrlRow, TRowVer1>>
        >);

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);
            const auto& row = reader->GetRow<TUrlRow>();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        reader->Next();

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 1);
            const auto& row = reader->GetRow<TRowVer1>();
            UNIT_ASSERT_VALUES_EQUAL(row.GetString_1(), "String");
            UNIT_ASSERT_VALUES_EQUAL(row.GetUint32_2(), 32);
        }
        reader->Next();

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 1);
            const auto& row = reader->GetRow<TRowVer1>();
            UNIT_ASSERT_VALUES_EQUAL(row.GetString_1(), "String");
            UNIT_ASSERT_VALUES_EQUAL(row.GetUint32_2(), 32);
        }
        reader->Next();

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 2);
            const auto& row = reader->GetRow<TUrlRow>();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        reader->Next();

        UNIT_ASSERT(!reader->IsValid());
    }

    Y_UNIT_TEST(ProtobufMultiTableHomo)
    {
        TUrlRow row;
        row.SetHost("http://www.example.com");
        row.SetPath("/");
        row.SetHttpCode(302);
        
        auto data = SerializeProto(row) +
            "\xFF\xFF\xFF\xFF" + TString("\x01\x00\x00\x00", 4) + // Table index
            SerializeProto(row) +
            SerializeProto(row) +
            "\xFF\xFF\xFF\xFF" + TString("\x02\x00\x00\x00", 4) + // Table index
            SerializeProto(row);

        auto stream = TMemoryInput(data);
        auto reader = CreateProtoMultiTableReader<TUrlRow>(&stream, 3);

        for (auto expectedIndex : TVector<int>{0, 1, 1, 2}) {
            UNIT_ASSERT(reader->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), expectedIndex);
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
            reader->Next();
        }
    }

    Y_UNIT_TEST(Yson)
    {
        auto data = "{x=1;y=2};{x=2;y=3}";

        auto stream = TMemoryInput(data);
        auto reader = CreateTableReader<TNode>(&stream);

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row["x"].AsInt64(), 1);
            UNIT_ASSERT_VALUES_EQUAL(row["y"].AsInt64(), 2);
        }
        reader->Next();

        UNIT_ASSERT(reader->IsValid());
        {
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row["x"].AsInt64(), 2);
            UNIT_ASSERT_VALUES_EQUAL(row["y"].AsInt64(), 3);
        }
        reader->Next();

        UNIT_ASSERT(!reader->IsValid());
    }
}
