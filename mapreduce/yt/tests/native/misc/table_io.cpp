#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/http/abortable_http_response.h>
#include <mapreduce/yt/http/host_manager.h>

#include <mapreduce/yt/io/proto_table_reader.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <util/random/fast.h>

#include <type_traits>

using namespace NYT;
using namespace NYT::NTesting;

static TString RandomBytes()
{
    static TReallyFastRng32 RNG(42);
    ui64 value = RNG.GenRand64();
    return TString((const char*)&value, sizeof(value));
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
        path.AddRange(
                TReadRange()
                    .LowerLimit(TReadLimit().RowIndex(10))
                    .UpperLimit(TReadLimit().RowIndex(20))
            ).AddRange(
                TReadRange()
                    .LowerLimit(TReadLimit().Key(1030))
                    .UpperLimit(TReadLimit().Key(1040))
            ).AddRange(
                TReadRange()
                    .LowerLimit(TReadLimit().RowIndex(50))
                    .UpperLimit(TReadLimit().RowIndex(60))
            ).AddRange(
                TReadRange()
                    .LowerLimit(TReadLimit().Key(1070))
                    .UpperLimit(TReadLimit().Key(1080))
            ).AddRange(
                TReadRange()
                    .Exact(TReadLimit().RowIndex(90))
            ).AddRange(
                TReadRange()
                    .Exact(TReadLimit().Key(1095))
            );

        TVector<i64> actualKeys;
        TVector<i64> actualRowIndices;
        TVector<ui32> actualRangeIndicies;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actualKeys.push_back(row["key"].AsInt64());
            actualRowIndices.push_back(reader->GetRowIndex());
            actualRangeIndicies.push_back(reader->GetRangeIndex());
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
        UNIT_ASSERT_VALUES_EQUAL(actualRangeIndicies, expectedRangeIndicies);
    }
    INSTANTIATE_NODE_READER_TESTS(ReadMultipleRangesNode)

#undef INSTANTIATE_NODE_READER_TESTS

    // TODO(levysotsky): Enable this test when packages are updated.
    void Descending()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = TRichYPath(workingDir + "/table");
        auto pathWithSchema = TRichYPath(path)
            .Schema(TTableSchema()
                .Strict(true)
                .AddColumn(TColumnSchema().Name("key").Type(VT_INT64).SortOrder(SO_DESCENDING)));
        {
            auto writer = client->CreateTableWriter<TNode>(pathWithSchema);
            for (int i = 100; i >= 50; --i) {
                writer->AddRow(TNode()("key", i));
            }
            writer->Finish();
        }
        {
            auto writer = client->CreateTableWriter<TNode>(path.Append(true));
            for (int i = 50; i < 100; ++i) {
                writer->AddRow(TNode()("key", i));
            }
            UNIT_ASSERT_EXCEPTION(writer->Finish(), yexception);
        }
        {
            auto writer = client->CreateTableWriter<TNode>(path.Append(true));
            for (int i = 49; i >= 0; --i) {
                writer->AddRow(TNode()("key", i));
            }
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(path);
        int i = 100;
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row["key"].AsInt64(), i);
            --i;
        }
    }

    // TODO(levysotsky): Enable this test when packages are updated.
    void KeyBound()
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto path = TRichYPath(workingDir + "/table")
                .Schema(TTableSchema()
                    .Strict(true)
                    .AddColumn(TColumnSchema().Name("key1").Type(VT_INT64).SortOrder(SO_ASCENDING))
                    .AddColumn(TColumnSchema().Name("key2").Type(VT_INT64).SortOrder(SO_ASCENDING)));
            auto writer = client->CreateTableWriter<TNode>(path);

            for (int i = 0; i < 100; ++i) {
                writer->AddRow(TNode()("key1", i / 10)("key2", i % 10));
            }
            writer->Finish();
        }

        TRichYPath path(workingDir + "/table");
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().KeyBound(TKeyBound(ERelation::GreaterOrEqual, TKey(2))))
            .UpperLimit(TReadLimit().KeyBound(TKeyBound(ERelation::Less, TKey(3)))));
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().KeyBound(TKeyBound(ERelation::Greater, TKey(4, 3))))
            .UpperLimit(TReadLimit().KeyBound(TKeyBound(ERelation::LessOrEqual, TKey(5, 2)))));
        path.AddRange(TReadRange()
            .LowerLimit(TReadLimit().KeyBound(TKeyBound(ERelation::Greater, TKey(9, 0)))));

        TVector<std::pair<i64, i64>> actualKeys;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actualKeys.emplace_back(row["key1"].AsInt64(), row["key2"].AsInt64());
        }

        const TVector<std::pair<i64, i64>> expectedKeys = {
            {2,0}, {2,1}, {2,2}, {2,3}, {2,4}, {2,5}, {2,6}, {2,7}, {2,8}, {2,9},
            {4,4}, {4,5}, {4,6}, {4,7}, {4,8}, {4,9}, {5,0}, {5,1}, {5,2},
            {9,1}, {9,2}, {9,3}, {9,4}, {9,5}, {9,6}, {9,7}, {9,8}, {9,9},
        };
        UNIT_ASSERT_VALUES_EQUAL(actualKeys, expectedKeys);
    }

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
        NYT::NPrivate::THostManager::Get().Reset();

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
            for (const auto& cursor : *reader) {
                Y_UNUSED(cursor);
            }
            UNIT_FAIL("Expected exception!!!!");
        } catch (std::exception& ex) {
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
        Y_PROTOBUF_SUPPRESS_NODISCARD row.SerializeToString(&data);
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
