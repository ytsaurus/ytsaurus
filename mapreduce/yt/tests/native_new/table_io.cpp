#include "lib.h"

#include <mapreduce/yt/tests/native_new/row.pb.h>

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

#include <util/random/fast.h>

using namespace NYT;
using namespace NYT::NTesting;

class TConfigSaver
{
public:
    TConfigSaver()
        : OldConfig_(*TConfig::Get())
    { }

    ~TConfigSaver()
    {
        *TConfig::Get() = OldConfig_;
    }

private:
    NYT::TConfig OldConfig_;
};

static TString RandomBytes() {
    static TReallyFastRng32 RNG(42);
    ui64 value = RNG.GenRand64();
    return TString((const char*)&value, sizeof(value));
}

SIMPLE_UNIT_TEST_SUITE(TableIo) {
    SIMPLE_UNIT_TEST(Simple)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>("//testing/table");
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(NonEmptyColumns)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table").Columns({"key1", "key3"}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("key1", "value1")("key3", "value3"));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(EmptyColumns)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table").Columns({}));
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode::CreateMap());
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(Move)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("key1", "value1")("key2", "value2")("key3", "value3"));
            writer->AddRow(TNode()("key1", "value4")("key2", "value5")("key3", "value6"));
            writer->Finish();
        }
        auto reader = client->CreateTableReader<TNode>(TRichYPath("//testing/table"));
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

    SIMPLE_UNIT_TEST(Protobuf)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>("//testing/table");
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 302));
            writer->AddRow(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TUrlRow>("//testing/table");
        UNIT_ASSERT(reader->IsValid());
        {
            const auto& row = reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_NO_EXCEPTION(reader->GetRow());
        {
            TUrlRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 302);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TUrlRow row;
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
            TUrlRow row;
            reader->MoveRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetHost(), "http://www.example.com");
            UNIT_ASSERT_VALUES_EQUAL(row.GetPath(), "/index.php");
            UNIT_ASSERT_VALUES_EQUAL(row.GetHttpCode(), 200);
        }
        UNIT_ASSERT_EXCEPTION(reader->GetRow(), yexception);
        {
            TUrlRow row;
            UNIT_ASSERT_EXCEPTION(reader->MoveRow(&row), yexception);
        }
        UNIT_ASSERT(reader->IsValid());

        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }

    SIMPLE_UNIT_TEST(ProtobufVersions)
    {
        auto client = CreateTestClient();
        {
            const auto writer = client->CreateTableWriter<TRowVer1>("//testing/ver1");
            TRowVer1 data;
            data.SetString_1("Ver1_String_1");
            data.SetUint32_2(0x12);
            writer->AddRow(data);
            writer->Finish();
        }

        //V1 as V2
        {
            const auto reader = client->CreateTableReader<TRowVer2>("//testing/ver1");
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
            const auto writer = client->CreateTableWriter<TRowVer2>("//testing/ver2");
            TRowVer2 data;
            data.SetString_1("Ver2_String_1");
            data.SetUint32_2(0x22);
            data.SetFixed64_3(0x23);
            writer->AddRow(data);
            writer->Finish();
        }

        //V2 as V1
        {
            const auto reader = client->CreateTableReader<TRowVer1>("//testing/ver2");
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

    SIMPLE_UNIT_TEST(ErrorInTableWriter)
    {
        const TNode DATA = TString(1024, 'a');
        auto client = CreateTestClient();
        client->Create("//testing/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>("//testing/table");
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

    SIMPLE_UNIT_TEST(ErrorInFinish)
    {
        auto client = CreateTestClient();
        client->Create("//testing/table", NT_TABLE, TCreateOptions().Force(true).Attributes(
                TNode()("schema",
                    TNode()
                    .Add(TNode()("name", "value")("type", "string")))
                ));

        auto writer = client->CreateTableWriter<TNode>("//testing/table");
        writer->AddRow(TNode()("bar", "qux"));
        UNIT_ASSERT_EXCEPTION(writer->Finish(), TErrorResponse);

        auto writeMore = [&] {
            writer->AddRow(TNode()("value", "a"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(writeMore(), TApiUsageError);
    }

    SIMPLE_UNIT_TEST(CantWriteAfterFinish)
    {
        auto client = CreateTestClient();
        auto writer = client->CreateTableWriter<TNode>("//testing/table");
        writer->AddRow(TNode()("value", "foo"));
        writer->Finish();
        UNIT_ASSERT_EXCEPTION(writer->AddRow(TNode()("value", "a")), TApiUsageError);
    }

    SIMPLE_UNIT_TEST(EmptyHosts)
    {
        TConfigSaver configSaver;

        auto client =CreateTestClient();
        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/table").SortedBy("key"));

            for (int i = 0; i < 10; ++i) {
                writer->AddRow(TNode()("key", i));
            }
            writer->Finish();
        }

        TConfig::Get()->Hosts = "hosts?role=ERROR";
        TConfig::Get()->UseHosts = true;
        TConfig::Get()->RetryCount = 1;

        {
            auto tx = client->StartTransaction();
            UNIT_ASSERT_EXCEPTION(tx->CreateTableReader<NYT::TNode>("//testing/table"), yexception);
        }
        {
            auto tx = client->StartTransaction();
            auto write = [=] {
                auto writer = tx->CreateTableWriter<NYT::TNode>("//testing/table");
                writer->AddRow(TNode()("key", 0));
                writer->Finish();
            };
            UNIT_ASSERT_EXCEPTION(write(), yexception);
        }
    }

    SIMPLE_UNIT_TEST(ReadErrorInTrailers)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<NYT::TNode>("//testing/table");
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

        auto reader = client->CreateTableReader<NYT::TYaMRRow>("//testing/table");

        // we expect first record to be read ok and error will come only later
        // in http trailer
        UNIT_ASSERT(reader->IsValid());

        auto readRemaining = [&]() {
            for (; reader->IsValid() ; reader->Next()) {
            }
        };
        UNIT_ASSERT_EXCEPTION(readRemaining(), NYT::TErrorResponse);
    }

    SIMPLE_UNIT_TEST(ReadUncanonicalPath)
    {
        auto client = CreateTestClient();
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath("//testing/table").SortedBy("key"));

        for (int i = 0; i < 100; ++i) {
            writer->AddRow(TNode()("key", i));
        }
        writer->Finish();

        TRichYPath path("//testing/table[#10:#20,30:40,#50:#60,70:80,#90,95]");

        yvector<i64> actual;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actual.push_back(row["key"].AsInt64());
        }

        const yvector<i64> expected = {
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            90,
            95,
        };
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    SIMPLE_UNIT_TEST(ReadMultipleRangesNode)
    {
        auto client = CreateTestClient();
        auto writer = client->CreateTableWriter<TNode>(
            TRichYPath("//testing/table").SortedBy("key"));

        for (int i = 0; i < 100; ++i) {
            writer->AddRow(TNode()("key", 1000 + i));
        }
        writer->Finish();

        TRichYPath path("//testing/table");
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

        yvector<i64> actualKeys;
        yvector<i64> actualRowIndices;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actualKeys.push_back(row["key"].AsInt64());
            actualRowIndices.push_back(reader->GetRowIndex());
        }

        const yvector<i64> expectedKeys = {
            1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019,
            1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039,
            1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059,
            1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079,
            1090,
            1095,
        };
        const yvector<i64> expectedRowIndices = {
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            90,
            95,
        };
        UNIT_ASSERT_VALUES_EQUAL(actualKeys, expectedKeys);
        UNIT_ASSERT_VALUES_EQUAL(actualRowIndices, expectedRowIndices);
    }

    SIMPLE_UNIT_TEST(TableLockedForWriterLifetime)
    {
        auto client = CreateTestClient();
        {
            auto path = TRichYPath("//testing/table").Append(false);
            auto writer = client->CreateTableWriter<TNode>(path);
            UNIT_ASSERT_EXCEPTION(client->CreateTableWriter<TNode>(path), TErrorResponse);
            writer->Finish();
        }
        {
            auto path = TRichYPath("//testing/table").Append(true);
            auto firstWriter = client->CreateTableWriter<TNode>(path);
            // we expect no exception here
            auto secondWriter = client->CreateTableWriter<TNode>(path);
            firstWriter->AddRow(TNode()("key", 100500));
            secondWriter->AddRow(TNode()("key", 2001000));
            firstWriter->Finish();
            secondWriter->Finish();
        }
    }
}
