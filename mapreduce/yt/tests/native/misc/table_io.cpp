#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/tests/native/proto_lib/all_types.pb.h>
#include <mapreduce/yt/tests/native/proto_lib/row.pb.h>

#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/io.h>

#include <mapreduce/yt/http/abortable_http_response.h>

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

    SIMPLE_UNIT_TEST(UntypedProtobufWriter)
    {
        auto client = CreateTestClient();
        {
            TUrlRow row;
            row.SetHost("http://www.example.com");
            row.SetPath("/index.php");
            row.SetHttpCode(200);
            const Message* ptrWithoutType = &row;

            auto writer = client->CreateTableWriter("//testing/urls", *TUrlRow::descriptor());
            writer->AddRow(*ptrWithoutType);
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TUrlRow>("//testing/urls");
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

        auto client = CreateTestClient();
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
        TConfig::Get()->ReadRetryCount = 1;

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

        TVector<i64> actualKeys;
        TVector<i64> actualRowIndices;
        auto reader = client->CreateTableReader<TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            const auto& row = reader->GetRow();
            actualKeys.push_back(row["key"].AsInt64());
            actualRowIndices.push_back(reader->GetRowIndex());
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
            UNIT_ASSERT_EXCEPTION(client->StartTransaction()->Lock(path.Path_, LM_EXCLUSIVE), TErrorResponse);
            // however we don't expect any exception here
            auto secondWriter = client->CreateTableWriter<TNode>(path);
            firstWriter->AddRow(TNode()("key", 100500));
            secondWriter->AddRow(TNode()("key", 2001000));
            firstWriter->Finish();
            secondWriter->Finish();
        }
    }

    size_t GetNumTransactions(IClientBasePtr client) {
        return client->Get("//sys/transactions").AsMap().size();
    }

    SIMPLE_UNIT_TEST(OptionallyCreateChildTransactionForIO)
    {
        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table");
        auto numTransactionsBefore = GetNumTransactions(client);
        {
            auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions().CreateTransaction(false));
            writer->AddRow(TNode()("key", 100500));
            UNIT_ASSERT_VALUES_EQUAL(GetNumTransactions(client), numTransactionsBefore);
            writer->Finish();
        }
        {
            auto reader = client->CreateTableReader<TNode>(path, TTableReaderOptions().CreateTransaction(false));
            reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(GetNumTransactions(client), numTransactionsBefore);
        }
        {
            // CreateTransaction default is true
            auto writer = client->CreateTableWriter<TNode>(path, TTableWriterOptions());
            writer->AddRow(TNode()("key", 2001000));
            UNIT_ASSERT_VALUES_EQUAL(GetNumTransactions(client), numTransactionsBefore + 1);
            writer->Finish();
        }
        {
            // CreateTransaction default is true
            auto reader = client->CreateTableReader<TNode>(path, TTableReaderOptions());
            reader->GetRow();
            UNIT_ASSERT_VALUES_EQUAL(GetNumTransactions(client), numTransactionsBefore + 1);
        }
        // TableWriter will never write to global transaction if created under a local one
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

    SIMPLE_UNIT_TEST(ReaderTakesLockOnTableIdNotPath)
    {
        TConfig::Get()->UseAbortableResponse = true;

        auto client = CreateTestClient();
        auto firstPath = TRichYPath("//testing/table1");
        auto secondPath = TRichYPath("//testing/table2");
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
            UNIT_ASSERT(reader->GetRow().AsMap().has("first_key"));
        }
    }

    SIMPLE_UNIT_TEST(UnsuccessfulRetries)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 3;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(0);

        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table");
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

    SIMPLE_UNIT_TEST(SuccessfulRetries)
    {
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 4;

        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table");
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

    SIMPLE_UNIT_TEST(TableReaderFromInputStream)
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

    SIMPLE_UNIT_TEST(ReadingWritingProtobufAllTypes)
    {
        auto oldUseClientProtobuf = TConfig::Get()->UseClientProtobuf;
        TConfig::Get()->UseClientProtobuf = false;

        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/proto_table");
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

        TConfig::Get()->UseClientProtobuf = oldUseClientProtobuf;
    }

    SIMPLE_UNIT_TEST(SimpleRetrylessWriter)
    {
        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table");
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

    SIMPLE_UNIT_TEST(RetrylessWriterAndLockedTable)
    {
        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table");
        auto lockingWriter = client->CreateTableWriter<TNode>(path);
        lockingWriter->AddRow(TNode()("key", "kluch"));

        auto retrylessWriter = client->CreateTableWriter<TNode>(path, TTableWriterOptions().SingleHttpRequest(true));
        auto writeMuchData = [&retrylessWriter] {
            TString row = "0123456789ABCDEF"; // 16 bytes
            for (int i = 0; i < (5 << 20); ++i) { // 5M * 16B = 80MB > 64MB
                retrylessWriter->AddRow(TNode()("key", row));
            }
        };
        UNIT_ASSERT_EXCEPTION(writeMuchData(), yexception);
        UNIT_ASSERT_NO_EXCEPTION(retrylessWriter->Finish()); // It's already finished
        UNIT_ASSERT_NO_EXCEPTION(lockingWriter->Finish());
    }

    void TestCompressionCodec(EEncoding encoding)
    {
        TConfigSaverGuard configGuard;

        TConfig::Get()->ContentEncoding = encoding;
        auto client = CreateTestClient();
        auto path = "//testing/table";

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

    SIMPLE_UNIT_TEST(CompressionCodecIdentity)
    {
        TestCompressionCodec(E_IDENTITY);
    }

    SIMPLE_UNIT_TEST(CompressionCodecGzip)
    {
        TestCompressionCodec(E_GZIP);
    }

    SIMPLE_UNIT_TEST(CompressionCodecBrotli)
    {
        TestCompressionCodec(E_BROTLI);
    }

    SIMPLE_UNIT_TEST(AbortWriter)
    {
        auto client = CreateTestClient();
        const int numRows = 2117;
        for (auto singleRequest : {true, false}) {
            auto path = TRichYPath("//testing/table" + ToString(singleRequest));
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

    SIMPLE_UNIT_TEST(ProtobufWriteAutoflush)
    {
        auto client = CreateTestClient();

        auto writer = client->CreateTableWriter<TUrlRow>("//testing/table", TTableWriterOptions().CreateTransaction(false));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/table/@row_count").AsInt64(), 0);
        TUrlRow row;
        for (size_t i = 0; i != 128; ++i) {
            row.SetHost(TString(1024 * 1024, 'a'));
            writer->AddRow(row);
        }

        UNIT_ASSERT(client->Get("//testing/table/@row_count").AsInt64() > 0);
    }
}

SIMPLE_UNIT_TEST_SUITE(BlobTableIo) {
    SIMPLE_UNIT_TEST(Simple)
    {
        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1024 * 1024 * 4, 'b'),
            TString(1024 * 1024 * 4, 'c'),
            TString(1027, 'd'),
        };

        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            };

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
            auto reader = client->CreateBlobTableReader("//testing/table", {"myfile_small"});
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), "small");
        }

        {
            TString expected;
            for (const auto& part : testDataParts) {
                expected += part;
            }
            auto reader = client->CreateBlobTableReader("//testing/table", {"myfile_big"});
            UNIT_ASSERT_EQUAL(reader->ReadAll(), expected);
        }
    }

    SIMPLE_UNIT_TEST(WrongPartSize)
    {
        const std::vector<TString> testDataParts = {
            TString(1024 * 1024 * 4, 'a'),
            TString(1027, 'd'),
        };

        auto client = CreateTestClient();

        {
            auto writer = client->CreateTableWriter<TNode>(
                TRichYPath("//testing/table").Schema(TTableSchema()
                    .AddColumn("filename", VT_STRING, SO_ASCENDING)
                    .AddColumn("part_index", VT_INT64, SO_ASCENDING)
                    .AddColumn("data", VT_STRING)));

            for (size_t i = 0; i != testDataParts.size(); ++i) {
                TNode row;
                row["filename"] = "myfile_big";
                row["part_index"] = static_cast<i64>(i);
                row["data"] = testDataParts[i];
                writer->AddRow(row);
            };

            writer->Finish();
        }


        auto readFile = [&] (ui64 partSize) {
            auto reader = client->CreateBlobTableReader("//testing/table", {"myfile_big"}, TBlobTableReaderOptions().PartSize(partSize));
            reader->ReadAll();
        };
        readFile(4 * 1024 * 1024); // no exception
        UNIT_ASSERT_EXCEPTION(readFile(100500), yexception);
    }
}
