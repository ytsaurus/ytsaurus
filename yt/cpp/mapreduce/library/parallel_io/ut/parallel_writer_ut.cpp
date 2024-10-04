#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/library/parallel_io/parallel_writer.h>

#include <yt/cpp/mapreduce/library/parallel_io/ut/proto/data.pb.h>

#include <yt/cpp/mapreduce/tests/lib/owning_yamr_row.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/user.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

bool CompTNode(const TNode& x, const TNode& y)
{
    return x["foo"].AsString() < y["foo"].AsString();
}

bool CompTOwningYaMRRow(const NTest::TOwningYaMRRow& x, const NTest::TOwningYaMRRow& y)
{
    return x.Key < y.Key;
}

bool CompTEmailRecord(const TEmailRecord& x, const TEmailRecord& y)
{
    return x.name() < y.name();
}

} // namespace

TEST(TParallelUnorderedWriterTest, NodeTest)
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
    EXPECT_EQ(read, write);
}

TEST(TParallelUnorderedWriterTest, NodeBatchTest)
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
    EXPECT_EQ(read, expected);
}

TEST(TParallelUnorderedWriterTest, MemoryLimitSingleProducer)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/parallel_writer_test1";

    auto writer = CreateParallelUnorderedTableWriter<TNode>(client, path,
        TParallelTableWriterOptions()
            .ThreadCount(10)
            .RamLimiter(::MakeIntrusive<TResourceLimiter>(static_cast<size_t>(2u))));

    TVector<TNode> task1 = {
        TNode()("foo", "bar_1"),
        TNode()("foo", "baz_1"),
        TNode()("foo", "ban_1")
    };
    TNode task2 = TNode()("foo", "bar_2");

    writer->AddRowBatch(task1, 0, 2);
    writer->AddRow(task2, 0, 2);

    writer->Finish();

    auto reader = client->CreateTableReader<TNode>(path);
    TVector<TNode> read;
    for (auto &cursor : *reader) {
        read.push_back(cursor.GetRow());
    }
    TVector<TNode> expected = task1;
    expected.push_back(task2);
    Sort(expected.begin(), expected.end(), CompTNode);
    Sort(read.begin(), read.end(), CompTNode);
    EXPECT_EQ(read, expected);
}

TEST(TParallelUnorderedWriterTest, MemoryLimitWithBuffers)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/parallel_writer_test1";

    auto options = TParallelTableWriterOptions()
            .ThreadCount(2)
            .AcquireRamForBuffers(true);
    auto limiter = ::MakeIntrusive<TResourceLimiter>(static_cast<size_t>(2u + options.TableWriterOptions_.BufferSize_ * 8));
    options.RamLimiter(limiter);

    auto writer = CreateParallelUnorderedTableWriter<TNode>(client, path, std::move(options));

    TVector<TNode> task1 = {
        TNode()("foo", "bar_1"),
        TNode()("foo", "baz_1"),
        TNode()("foo", "ban_1")
    };
    TNode task2 = TNode()("foo", "bar_2");

    writer->AddRowBatch(task1, 0, 2);
    writer->AddRow(task2, 0, 2);

    writer->Finish();

    auto reader = client->CreateTableReader<TNode>(path);
    TVector<TNode> read;
    for (auto &cursor : *reader) {
        read.push_back(cursor.GetRow());
    }
    TVector<TNode> expected = task1;
    expected.push_back(task2);
    Sort(expected.begin(), expected.end(), CompTNode);
    Sort(read.begin(), read.end(), CompTNode);
    EXPECT_EQ(read, expected);
}

TEST(TParallelUnorderedWriterTest, MemoryLimitNoMemoryForBuffers)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/parallel_writer_test1";

    auto options = TParallelTableWriterOptions()
            .ThreadCount(10)
            .AcquireRamForBuffers(true);
    auto limiter = ::MakeIntrusive<TResourceLimiter>(static_cast<size_t>(2u + options.TableWriterOptions_.BufferSize_ * 3));
    options.RamLimiter(limiter);

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        CreateParallelUnorderedTableWriter<TNode>(client, path, std::move(options)),
        yexception,
        "acquire 268435456 >= Limit_ - CurrentHardUsage_"
    );
}

TEST(TParallelUnorderedWriterTest, BadRowWeight)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/table";

    auto writer = CreateParallelUnorderedTableWriter<TNode>(client, path, TParallelTableWriterOptions()
                .RamLimiter(::MakeIntrusive<TResourceLimiter>(10u)));

    TNode task = TNode()("foo", "bar");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            writer->AddRow(task, 0, 11),
            TApiUsageError,
            ""
    );

    writer->AddRow(task, 0, 5);
    writer->Finish();
}

TEST(TParallelUnorderedWriterTest, YaMRRowTest)
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
    EXPECT_EQ(writeOwning, readOwning);
}

TEST(TParallelUnorderedWriterTest, ProtoTest)
{
    TVector<TEmailRecord> write;
    TEmailRecord emailRecord;
    emailRecord.set_name("name");
    emailRecord.set_email("mail");
    write.push_back(emailRecord);
    emailRecord.set_name("name2");
    emailRecord.set_email("mail2");
    write.push_back(emailRecord);
    emailRecord.set_name("name3");
    emailRecord.set_email("mail3");
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
    EXPECT_EQ(read.size(), write.size());
    for (size_t i = 0; i < read.size(); ++i) {
        EXPECT_EQ(read[i].name(), write[i].name());
        EXPECT_EQ(read[i].email(), write[i].email());
    }
}

TEST(TParallelUnorderedWriterTest, Schema)
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

    EXPECT_THROW(addRowWithRusColumn(), TErrorResponse);

    // Can't change type of column.
    EXPECT_THROW(
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

TEST(TParallelUnorderedWriterTest, ErrorInFinish)
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
    EXPECT_THROW(writer->Finish(), TErrorResponse);

    auto writeMore = [&] {
        writer->AddRow(TNode()("value", "a"));
        writer->Finish();
    };

    EXPECT_THROW(writeMore(), TApiUsageError);
}

TEST(TParallelUnorderedWriterTest, CantWriteAfterFinish)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto writer = CreateParallelUnorderedTableWriter<TNode>(client, workingDir + "/table");
    writer->AddRow(TNode()("value", "foo"));
    writer->Finish();
    EXPECT_THROW(writer->AddRow(TNode()("value", "a")), TApiUsageError);
}

TEST(TParallelUnorderedWriterTest, TableLockedForWriterLifetime)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto path = TRichYPath(workingDir + "/table").Append(false);
        auto writer = CreateParallelUnorderedTableWriter<TNode>(client, path);
        EXPECT_THROW(CreateParallelUnorderedTableWriter<TNode>(client, path), TErrorResponse);
        writer->Finish();
    }
    {
        auto path = TRichYPath(workingDir + "/table").Append(true);
        auto firstWriter = CreateParallelUnorderedTableWriter<TNode>(client, path);
        EXPECT_THROW(client->StartTransaction()->Lock(path.Path_, LM_EXCLUSIVE), TErrorResponse);
        // however we don't expect any exception here
        auto secondWriter = CreateParallelUnorderedTableWriter<TNode>(client, path);
        firstWriter->AddRow(TNode()("key", 100500));
        secondWriter->AddRow(TNode()("key", 2001000));
        firstWriter->Finish();
        secondWriter->Finish();
    }
}

TEST(TParallelUnorderedWriterTest, ErrorInTableWriter)
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
    EXPECT_THROW(writeTable(), TErrorResponse);
}

TEST(TParallelUnorderedWriterTest, PathWithAppend)
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
    EXPECT_EQ(reader->GetRow(), TNode()("a", 1));
    reader->Next();
    EXPECT_TRUE(!reader->IsValid());
}

class TParallelUnorderedWriterDeadlockFinishTest_YTSAURUSSUP_767
    : public ::testing::TestWithParam<bool>
{ };

TEST_P(TParallelUnorderedWriterDeadlockFinishTest_YTSAURUSSUP_767, WriteDeadlock)
{
    bool finish = GetParam();
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    const TString table = workingDir + "/parallel_writer_test_path_with_append";

    auto goodBatch = TVector<TNode>(10, TNode()("a", TString(1_MB, 'a')));
    auto badBatch = goodBatch;
    // We set invalid row and it is important that error happens in the begin of this batch.
    badBatch[0] = TNode(5);

    for (int i = 0; i < 30; ++i) {
        // We are trying to catch a race here. See below.
        auto writer = CreateParallelUnorderedTableWriter<TNode>(
            client,
            table,
            TParallelTableWriterOptions()
                .ThreadCount(1)
                .TableWriterOptions(TTableWriterOptions().BufferSize(1_MB))
        );

        writer->AddRowBatch(goodBatch);
        writer->AddRowBatch(badBatch);
        try {
            writer->AddRowBatch(goodBatch);
        } catch (const std::exception& ex) {
            // Writer can already finish processing previous badBatch
            // in that case we'll have exception here.
            //
            // But this case is not interesting we try again to reproduce the case
            // described in
            // https://github.com/ytsaurus/ytsaurus/issues/547
            EXPECT_THAT(ex.what(), ::testing::HasSubstr("Row should be a map node"));
            continue;
        }

        if (finish) {
            auto finish = [&] {
                writer->Finish();
            };
            EXPECT_THROW_MESSAGE_HAS_SUBSTR(finish(), std::exception, "Row should be a map node");
        } else {
            writer->Abort();
        }
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(WithFinish, TParallelUnorderedWriterDeadlockFinishTest_YTSAURUSSUP_767, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(WithoutFinish, TParallelUnorderedWriterDeadlockFinishTest_YTSAURUSSUP_767, ::testing::Values(false));
