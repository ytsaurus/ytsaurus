#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/library/parallel_io/parallel_writer.h>
#include <util/system/user.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yt/cpp/mapreduce/tests/lib/owning_yamr_row.h>
#include <yt/cpp/mapreduce/library/parallel_io/ut/data.pb.h>
#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

static bool CompTNode(const TNode& x, const TNode& y) {
    return x["foo"].AsString() < y["foo"].AsString();
}
static bool CompTOwningYaMRRow(const NTest::TOwningYaMRRow& x, const NTest::TOwningYaMRRow& y) {
    return x.Key < y.Key;
}
static bool CompTEmailRecord(const TEmailRecord& x, const TEmailRecord& y) {
    return x.GetName() < y.GetName();
}

Y_UNIT_TEST_SUITE(ParallelUnorderedWriter)
{
    Y_UNIT_TEST(NodeTest)
    {
        TVector<TNode> write = {
            TNode()("foo", "bar"),
            TNode()("foo", "baz"),
            TNode()("foo", "ban")
        };
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString table = workingDir + "/parallel_writer_test1";
        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, table);
        for (const auto &row : write) {
            writer->AddRow(row);
        }
        writer->Finish();
        auto reader = client->CreateTableReader<TNode>(table);
        TVector<TNode> read;
        for (auto &cursor : *reader) {
            read.push_back(cursor.GetRow());
        }
        Sort(write.begin(), write.end(), CompTNode);
        Sort(read.begin(), read.end(), CompTNode);
        UNIT_ASSERT_VALUES_EQUAL(read, write);
    }

    Y_UNIT_TEST(NodeBatchTest)
    {
        TVector<TNode> task1 = {
            TNode()("foo", "bar_1"),
            TNode()("foo", "baz_1"),
            TNode()("foo", "ban_1")
        };
        TNode task2 = TNode()("foo", "bar_2");
        TVector<TNode> task3 = {
            TNode()("foo", "bar_3"),
            TNode()("foo", "baz_3"),
            TNode()("foo", "ban_3")
        };

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString table = workingDir + "/parallel_writer_test1";
        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, table);
        writer->AddRowBatch(task1);
        writer->AddRow(task2);
        writer->AddRowBatch(task3);
        writer->Finish();
        auto reader = client->CreateTableReader<TNode>(table);
        TVector<TNode> read;
        for (auto &cursor : *reader) {
            read.push_back(cursor.GetRow());
        }
        TVector<TNode> expected = task1;
        expected.push_back(task2);
        std::copy(task3.begin(), task3.end(), std::back_inserter(expected));
        Sort(expected.begin(), expected.end(), CompTNode);
        Sort(read.begin(), read.end(), CompTNode);
        UNIT_ASSERT_VALUES_EQUAL(read, expected);
    }

    Y_UNIT_TEST(YaMRRowTest)
    {
        TVector<TYaMRRow> write = {
            TYaMRRow() = {
                .Key = "key",
                .Value = "value"
            },
            TYaMRRow() = {
                .Key = "key2",
                .Value = "value2"
            },
            TYaMRRow() = {
                .Key = "key3",
                .Value = "value3"
            }
        };
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString table = workingDir + "/parallel_writer_test2";
        auto writer = CreateParallelUnorderedTableWriter<TYaMRRow>(client, table);
        for (const auto &row : write) {
            writer->AddRow(row);
        }
        writer->Finish();
        auto reader = client->CreateTableReader<TYaMRRow>(table);
        TVector<NTest::TOwningYaMRRow> readOwning;
        for (auto &cursor : *reader) {
            readOwning.push_back(cursor.GetRow());
        }
        TVector<NTest::TOwningYaMRRow> writeOwning;
        for (const auto &row : write) {
            writeOwning.push_back(row);
        }
        Sort(writeOwning.begin(), writeOwning.end(), CompTOwningYaMRRow);
        Sort(readOwning.begin(), readOwning.end(), CompTOwningYaMRRow);
        UNIT_ASSERT_VALUES_EQUAL(writeOwning, readOwning);
    }

    Y_UNIT_TEST(ProtoTest)
    {
        TVector<TEmailRecord> write;
        TEmailRecord emailRecord;
        emailRecord.SetName("name");
        emailRecord.SetEmail("mail");
        write.push_back(emailRecord);
        emailRecord.SetName("name2");
        emailRecord.SetEmail("mail2");
        write.push_back(emailRecord);
        emailRecord.SetName("name3");
        emailRecord.SetEmail("mail3");
        write.push_back(emailRecord);
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString table = workingDir + "/parallel_writer_test3";
        auto writer = CreateParallelUnorderedTableWriter<TEmailRecord>(client, table);
        for (const auto &row : write) {
            writer->AddRow(row);
        }
        writer->Finish();
        auto reader = client->CreateTableReader<TEmailRecord>(table);
        TVector<TEmailRecord> read;
        for (auto &cursor : *reader) {
            read.push_back(cursor.GetRow());
        }
        Sort(write.begin(), write.end(), CompTEmailRecord);
        Sort(read.begin(), read.end(), CompTEmailRecord);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), write.size());
        for (size_t i = 0; i < read.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].GetName(), write[i].GetName());
            UNIT_ASSERT_VALUES_EQUAL(read[i].GetEmail(), write[i].GetEmail());
        }
    }

    Y_UNIT_TEST(Schema)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(
            workingDir + "/table", NT_TABLE, TCreateOptions()
            .Attributes(
                TNode()
                ("schema", TNode()
                .Add(TNode()("name", "eng")("type", "string"))
                .Add(TNode()("name", "number")("type", "int64")))));
        {
            auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
            writer->AddRow(TNode()("eng", "one")("number", 1));
            writer->Finish();
        }

        auto addRowWithRusColumn = [&] {
            auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
            writer->AddRow(TNode()("eng", "one")("number", 1)("rus", "odin"));
            writer->Finish();
        };

        UNIT_ASSERT_EXCEPTION(addRowWithRusColumn(), TErrorResponse);

        // Can't change type of column.
        UNIT_ASSERT_EXCEPTION(
            client->AlterTable(
                workingDir + "/table",
                TAlterTableOptions()
                    .Schema(TTableSchema()
                    .AddColumn(TColumnSchema().Name("eng").Type(VT_INT64))
                    .AddColumn(TColumnSchema().Name("number").Type(VT_INT64)))),
            TErrorResponse);

        client->AlterTable(
            workingDir + "/table",
            TAlterTableOptions()
                .Schema(TTableSchema()
                .AddColumn(TColumnSchema().Name("eng").Type(VT_STRING))
                .AddColumn(TColumnSchema().Name("number").Type(VT_INT64))
                .AddColumn(TColumnSchema().Name("rus").Type(VT_STRING))));

        // No exception.
        addRowWithRusColumn();
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

        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
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
        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
        writer->AddRow(TNode()("value", "foo"));
        writer->Finish();
        UNIT_ASSERT_EXCEPTION(writer->AddRow(TNode()("value", "a")), TApiUsageError);
    }

    Y_UNIT_TEST(TableLockedForWriterLifetime)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        {
            auto path = TRichYPath(workingDir + "/table").Append(false);
            auto writer = CreateParallelUnorderedTableWriter<TNode>(client, path);
            UNIT_ASSERT_EXCEPTION(CreateParallelUnorderedTableWriter<TNode>(client, path), TErrorResponse);
            writer->Finish();
        }
        {
            auto path = TRichYPath(workingDir + "/table").Append(true);
            auto firstWriter = CreateParallelUnorderedTableWriter<TNode>(client, path);
            UNIT_ASSERT_EXCEPTION(client->StartTransaction()->Lock(path.Path_, LM_EXCLUSIVE), TErrorResponse);
            // however we don't expect any exception here
            auto secondWriter = CreateParallelUnorderedTableWriter<TNode>(client, path);
            firstWriter->AddRow(TNode()("key", 100500));
            secondWriter->AddRow(TNode()("key", 2001000));
            firstWriter->Finish();
            secondWriter->Finish();
        }
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

        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
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

    Y_UNIT_TEST(PathWithAppend)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString table = workingDir + "/parallel_writer_test_path_with_append";
        {
            auto writer = CreateParallelUnorderedTableWriter<TNode>(client, table);
            writer->AddRow(TNode()("a", 1));
            writer->Finish();
        }

        auto reader = client->CreateTableReader<TNode>(table);
        UNIT_ASSERT_VALUES_EQUAL(reader->GetRow(), TNode()("a", 1));
        reader->Next();
        UNIT_ASSERT(!reader->IsValid());
    }
}
