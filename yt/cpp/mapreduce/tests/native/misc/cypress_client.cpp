#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/threading/future/async.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <util/thread/pool.h>

using namespace NYT;
using namespace NYT::NTesting;

static TNode::TListType SortedStrings(TNode::TListType input) {
    std::sort(input.begin(), input.end(), [] (const TNode& lhs, const TNode& rhs) {
        return lhs.AsString() < rhs.AsString();
    });
    return input;
}

template <typename T>
T MakeCopy(const T& t) {
    return t;
}

TEST(CypressClient, TestCreateAllTypes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const ENodeType nodeTypeList[] = {
        NT_STRING,
        NT_INT64,
        NT_UINT64,
        NT_DOUBLE,
        NT_BOOLEAN,
        NT_MAP,
        NT_LIST,
        NT_FILE,
        NT_TABLE,
        NT_DOCUMENT,
    };

    for (const auto nodeType : nodeTypeList) {
        auto nodeTypeStr = ToString(nodeType);
        const TString nodePath = workingDir + "/" + nodeTypeStr;
        const TString nodeTypePath = nodePath + "/@type";
        const TString nodeIdPath = nodePath + "/@id";

        auto nodeId = client->Create(nodePath, nodeType);
        EXPECT_EQ(client->Get(nodeTypePath), nodeTypeStr);
        EXPECT_EQ(client->Get(nodeIdPath), GetGuidAsString(nodeId));
    }

    client->Create("//sys/groups/idm-group:100500", NT_GROUP, TCreateOptions().Attributes(TNode()("name", "idm-group:100500")));
}

TEST(CypressClient, TestCreate)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Create(workingDir + "/map_node", NT_MAP);
    EXPECT_EQ(client->Exists(workingDir + "/map_node"), true);

    tx->Create(workingDir + "/tx_map_node", NT_MAP);
    EXPECT_EQ(client->Exists(workingDir + "/tx_map_node"), false);
    EXPECT_EQ(tx->Exists(workingDir + "/tx_map_node"), true);

    EXPECT_THROW(
        client->Create(workingDir + "/recursive_not_set_dir/node", NT_TABLE),
        TErrorResponse);
    EXPECT_EQ(client->Exists(workingDir + "/recursive_not_set_dir"), false);

    client->Create(workingDir + "/recursive_set_dir/node", NT_TABLE, TCreateOptions().Recursive(true));
    EXPECT_EQ(client->Exists(workingDir + "/recursive_set_dir"), true);

    client->Create(workingDir + "/existing_table", NT_TABLE);
    EXPECT_THROW(
        client->Create(workingDir + "/existing_table", NT_TABLE),
        TErrorResponse);
    client->Create(workingDir + "/existing_table", NT_TABLE, TCreateOptions().IgnoreExisting(true));
    EXPECT_THROW(
        client->Create(workingDir + "/existing_table", NT_MAP, TCreateOptions().IgnoreExisting(true)),
        TErrorResponse);

    client->Create(workingDir + "/node_with_attributes", NT_TABLE, TCreateOptions().Attributes(TNode()("attr_name", "attr_value")));
    EXPECT_EQ(
        client->Get(workingDir + "/node_with_attributes/@attr_name"),
        TNode("attr_value"));

    {
        auto initialNodeId = client->Create(workingDir + "/existing_table_for_force", NT_TABLE);

        auto nonForceNodeId = client->Create(workingDir + "/existing_table_for_force", NT_TABLE, TCreateOptions().IgnoreExisting(true));
        EXPECT_EQ(initialNodeId, nonForceNodeId);
        auto forceNodeId = client->Create(workingDir + "/existing_table_for_force", NT_TABLE, TCreateOptions().Force(true));
        EXPECT_TRUE(forceNodeId != initialNodeId);
    }
}

TEST(CypressClient, TestCreateProtobufTable)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto id = client->CreateTable<NYT::NTesting::TUrlRow>(
        workingDir + "/table",
        {"Host"});

    EXPECT_EQ(client->Get(workingDir + "/table/@id").AsString(), GetGuidAsString(id));

    auto schemaNode = client->Get(workingDir + "/table/@schema");
    TTableSchema schema;
    Deserialize(schema, schemaNode);

    EXPECT_EQ(std::ssize(schema.Columns()), 3);

    EXPECT_EQ(schema.Columns()[0].Name(), "Host");
    EXPECT_EQ(schema.Columns()[0].Type(), EValueType::VT_STRING);
    EXPECT_EQ(schema.Columns()[0].SortOrder(), ESortOrder::SO_ASCENDING);

    EXPECT_EQ(schema.Columns()[1].Name(), "Path");
    EXPECT_EQ(schema.Columns()[1].Type(), EValueType::VT_STRING);
    EXPECT_TRUE(schema.Columns()[1].SortOrder().Empty());

    EXPECT_EQ(schema.Columns()[2].Name(), "HttpCode");
    EXPECT_EQ(schema.Columns()[2].Type(), EValueType::VT_INT32);
    EXPECT_TRUE(schema.Columns()[2].SortOrder().Empty());
}

TEST(CypressClient, TestCreateHugeAttribute)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    const TString hugeAttribute(1024 * 1024, 'a');
    client->Create(workingDir + "/table", NT_TABLE,
        TCreateOptions().Attributes(TNode()("huge_attribute", hugeAttribute)));
    EXPECT_EQ(client->Get(workingDir + "/table/@huge_attribute").AsString(), hugeAttribute);
}

TEST(CypressClient, TestRemove)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Create(workingDir + "/table", NT_TABLE);
    client->Remove(workingDir + "/table");
    EXPECT_EQ(client->Exists(workingDir + "/table"), false);

    tx->Create(workingDir + "/tx_table", NT_TABLE);
    tx->Remove(workingDir + "/tx_table");
    EXPECT_EQ(tx->Exists(workingDir + "/tx_table"), false);

    client->Create(workingDir + "/map_node/table_node", NT_TABLE, TCreateOptions().Recursive(true));

    EXPECT_THROW(
        client->Remove(workingDir + "/map_node"),
        TErrorResponse);
    EXPECT_EQ(client->Exists(workingDir + "/map_node/table_node"), true);
    client->Remove(workingDir + "/map_node", TRemoveOptions().Recursive(true));
    EXPECT_EQ(client->Exists(workingDir + "/map_node"), false);

    EXPECT_THROW(
        client->Remove(workingDir + "/missing_node"),
        TErrorResponse);
    client->Remove(workingDir + "/missing_node", TRemoveOptions().Force(true));
}

TEST(CypressClient, TestSetGet)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    const TNode nodeList[] = {
        TNode("foobar"),
        TNode(ui64(42)),
        TNode(i64(-100500)),
        TNode(3.14),
        TNode(true),
        TNode(false),
        TNode().Add("gg").Add("lol").Add(100500),
        TNode()("key1", "value1")("key2", "value2"),
    };

    for (const auto& node : nodeList) {
        client->Remove(workingDir + "/node", TRemoveOptions().Recursive(true).Force(true));
        client->Set(workingDir + "/node", node);
        EXPECT_EQ(client->Get(workingDir + "/node"), node);
    }

    {
        TNode node("Recursive");
        EXPECT_THROW(client->Set(workingDir + "/node/with/some/path", node), yexception);
        client->Set(workingDir + "/node/with/some/path", node, TSetOptions().Recursive(true));
        EXPECT_EQ(client->Get(workingDir + "/node/with/some/path"), node);
    }
    {
        auto node = TNode()("key", "value");
        client->Remove(workingDir + "/node", TRemoveOptions().Force(true).Recursive(true));
        client->Create(workingDir + "/node", ENodeType::NT_MAP);
        // TODO(levysotsky): Uncomment when set will be forbidden by default.
        // EXPECT_THROW(client->Set(workingDir + "/node", node), yexception);
        client->Set(workingDir + "/node", node, TSetOptions().Force(true));
        EXPECT_EQ(client->Get(workingDir + "/node"), node);
    }

    auto tx = client->StartTransaction();
    tx->Set(workingDir + "/tx_node", TNode(10050));
    EXPECT_EQ(client->Exists(workingDir + "/tx_node"), false);
    EXPECT_EQ(tx->Get(workingDir + "/tx_node"), TNode(10050));

    client->Create(workingDir + "/node_with_attr", NT_TABLE);
    client->Set(workingDir + "/node_with_attr/@attr_name", TNode("attr_value"));

    auto nodeWithAttr = client->Get(workingDir + "/node_with_attr",
        TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("attr_name")));

    EXPECT_EQ(nodeWithAttr.GetAttributes().AsMap().at("attr_name"), TNode("attr_value"));
}

TEST(CypressClient, TestList)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();
    client->Set(workingDir + "/foo", 5);
    client->Set(workingDir + "/bar", "bar");
    client->Set(workingDir + "/bar", "bar");
    client->Set(workingDir + "/bar/@attr_name", "attr_value");
    tx->Set(workingDir + "/tx_qux", "gg");

    auto res = client->List(workingDir + "");

    EXPECT_EQ(
        SortedStrings(res),
        TNode::TListType({"bar", "foo"}));

    auto txRes = tx->List(workingDir + "");
    EXPECT_EQ(
        SortedStrings(txRes),
        TNode::TListType({"bar", "foo", "tx_qux"}));

    auto maxSizeRes = client->List(workingDir + "", TListOptions().MaxSize(1));
    EXPECT_EQ(std::ssize(maxSizeRes), 1);
    EXPECT_TRUE(THashSet<TString>({"foo", "bar"}).contains(maxSizeRes[0].AsString()));

    auto attrFilterRes = client->List(workingDir + "",
        TListOptions().AttributeFilter(TAttributeFilter().AddAttribute("attr_name")));
    attrFilterRes = SortedStrings(attrFilterRes);
    auto barNode = TNode("bar");
    barNode.Attributes()("attr_name", "attr_value");
    EXPECT_EQ(
        attrFilterRes,
        TNode::TListType({barNode, "foo"}));
}

// YT-10354
TEST(CypressClient, TestListEmptyAttributeFilter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();
    client->Set(workingDir + "/foo", 5);
    client->Set(workingDir + "/bar", "bar");

    NYT::TAttributeFilter filter;
    auto res = client->List(workingDir, NYT::TListOptions().AttributeFilter(std::move(filter)));

    EXPECT_EQ(
        SortedStrings(res),
        TNode::TListType({"bar", "foo"}));
}

TEST(CypressClient, TestCopy)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    client->Set(workingDir + "/simple", "simple value");
    client->Copy(workingDir + "/simple", workingDir + "/copy_simple");
    EXPECT_EQ(client->Get(workingDir + "/copy_simple"), client->Get(workingDir + "/simple"));
}

TEST(CypressClient, TestMove)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    client->Set(workingDir + "/simple", "simple value");
    auto oldValue = client->Get(workingDir + "/simple");
    client->Move(workingDir + "/simple", workingDir + "/moved_simple");
    EXPECT_EQ(client->Get(workingDir + "/moved_simple"), oldValue);
    EXPECT_EQ(client->Exists(workingDir + "/simple"), false);
}

TEST(CypressClient, TestCopy_PreserveExpirationTime)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const TString expirationTime = "2042-02-15T18:45:19.591902Z";
    for (TString path : {workingDir + "/table_default", workingDir + "/table_false", workingDir + "/table_true"}) {
        client->Create(path, NT_TABLE);
        client->Set(path + "/@expiration_time", expirationTime);
    }

    client->Copy(workingDir + "/table_default", workingDir + "/copy_table_default");
    client->Copy(workingDir + "/table_true", workingDir + "/copy_table_true", TCopyOptions().PreserveExpirationTime(true));
    client->Copy(workingDir + "/table_false", workingDir + "/copy_table_false", TCopyOptions().PreserveExpirationTime(false));

    EXPECT_THROW(client->Get(workingDir + "/copy_table_default/@expiration_time"), yexception);
    EXPECT_EQ(client->Get(workingDir + "/copy_table_true/@expiration_time"), expirationTime);
    EXPECT_THROW(client->Get(workingDir + "/copy_table_false/@expiration_time"), yexception);
}

TEST(CypressClient, TestMove_PreserveExpirationTime)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    const TString expirationTime = "2042-02-15T18:45:19.591902Z";
    for (TString path : {workingDir + "/table_default", workingDir + "/table_false", workingDir + "/table_true"}) {
        client->Create(path, NT_TABLE);
        client->Set(path + "/@expiration_time", expirationTime);
    }

    client->Move(workingDir + "/table_default", workingDir + "/moved_table_default");
    client->Move(workingDir + "/table_true", workingDir + "/moved_table_true", TMoveOptions().PreserveExpirationTime(true));
    client->Move(workingDir + "/table_false", workingDir + "/moved_table_false", TMoveOptions().PreserveExpirationTime(false));

    // TODO(levysotsky) Uncomment when default behaviour is stable
    // EXPECT_EQ(client->Get(workingDir + "/moved_table_default/@expiration_time"), TNode(expirationTime));
    EXPECT_EQ(client->Get(workingDir + "/moved_table_true/@expiration_time"), TNode(expirationTime));
    EXPECT_THROW(client->Get(workingDir + "/moved_table_false/@expiration_time"), yexception);
}

TEST(CypressClient, TestLink)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    client->Create(workingDir + "/table", NT_TABLE);
    client->Create(workingDir + "/table2", NT_TABLE);
    client->Link(workingDir + "/table", workingDir + "/table_link");

    EXPECT_EQ(client->Exists(workingDir + "/table"), true);
    EXPECT_EQ(client->Exists(workingDir + "/table_link"), true);
    EXPECT_EQ(client->Get(workingDir + "/table_link&/@target_path"), workingDir + "/table");

    EXPECT_THROW(client->Link(workingDir + "/table2", workingDir + "/table_link"), yexception);

    client->Link(workingDir + "/table2", workingDir + "/table_link", NYT::TLinkOptions().Force(true));
    EXPECT_EQ(client->Exists(workingDir + "/table2"), true);
    EXPECT_EQ(client->Get(workingDir + "/table_link&/@target_path"), workingDir + "/table2");
}

TEST(CypressClient, TestConcatenate)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateFileWriter(workingDir + "/file1");
        *writer << "foo";
        writer->Finish();
    }
    {
        auto writer = client->CreateFileWriter(workingDir + "/file2");
        *writer << "bar";
        writer->Finish();
    }
    {
        auto writer = client->CreateTableWriter<TNode>(workingDir + "/table1");
        writer->AddRow(TNode()("foo", "bar"));
        writer->Finish();
    }
    client->Create(workingDir + "/concat", NT_FILE);
    TVector<TYPath> files{workingDir + "/file1", workingDir + "/file2"};
    TVector<TYPath> tables{workingDir + "/table1", workingDir + "/table1"};
    client->Concatenate(files, workingDir + "/concat");
    {
        auto reader = client->CreateFileReader(workingDir + "/concat");
        EXPECT_EQ(reader->ReadAll(), "foobar");
    }
    client->Concatenate(files, workingDir + "/concat", TConcatenateOptions().Append(true));
    {
        auto reader = client->CreateFileReader(workingDir + "/concat");
        EXPECT_EQ(reader->ReadAll(), "foobarfoobar");
    }
    client->Concatenate(files, workingDir + "/concat", TConcatenateOptions().Append(true));
    {
        auto reader = client->CreateFileReader(workingDir + "/concat");
        EXPECT_EQ(reader->ReadAll(), "foobarfoobarfoobar");
    }

    // Nonexistent output file.
    client->Concatenate(files, workingDir + "/nonexistent_file", TConcatenateOptions());
    {
        auto reader = client->CreateFileReader(workingDir + "/nonexistent_file");
        EXPECT_EQ(reader->ReadAll(), "foobar");
    }

    // Nonexistent output table.
    client->Concatenate(tables, workingDir + "/nonexistent_table", TConcatenateOptions());
    {
        auto reader = client->CreateTableReader<TNode>(workingDir + "/nonexistent_table");
        EXPECT_EQ(reader->GetRow(), TNode()("foo", "bar"));
        EXPECT_EQ(reader->GetRow(), TNode()("foo", "bar"));
    }
}

TEST(CypressClient, TestTxConcatenate)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto writer = client->CreateFileWriter(workingDir + "/file1");
        *writer << "foo";
        writer->Finish();
    }
    {
        auto writer = client->CreateFileWriter(workingDir + "/file2");
        *writer << "bar";
        writer->Finish();
    }
    auto tx = client->StartTransaction();
    tx->Create(workingDir + "/concat", NT_FILE);
    tx->Concatenate({workingDir + "/file1", workingDir + "/file2"}, workingDir + "/concat");
    {
        auto reader = tx->CreateFileReader(workingDir + "/concat");
        EXPECT_EQ(reader->ReadAll(), "foobar");
    }
    EXPECT_TRUE(!client->Exists(workingDir + "/concat"));

    // Nonexistent output.
    tx->Concatenate({workingDir + "/file1", workingDir + "/file2"}, workingDir + "/nonexistent");
    {
        auto reader = tx->CreateFileReader(workingDir + "/nonexistent");
        EXPECT_EQ(reader->ReadAll(), "foobar");
    }
}

TEST(CypressClient, TestRetries)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->RetryCount = 4;
    client->Create(workingDir + "/table", NT_MAP);
    {
        auto outage = TAbortableHttpResponse::StartOutage("/set");
        EXPECT_THROW(client->Set(workingDir + "/table/@my_attr", 42), TAbortedForTestPurpose);
    }
    {
        auto outage = TAbortableHttpResponse::StartOutage("/set", TConfig::Get()->RetryCount - 1);
        EXPECT_NO_THROW(client->Set(workingDir + "/table/@my_attr", -43));
    }
}

TEST(CypressClient, TestGetColumnarStatistics)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();
    {
        auto writer = tx->CreateTableWriter<TNode>(workingDir + "/table");
        writer->AddRow(TNode()("foo", 1)("bar", "baz"));
        writer->AddRow(TNode()("foo", 2)("bar", "qux"));
        writer->Finish();
    }

    auto statisticsList = tx->GetTableColumnarStatistics({ TRichYPath(workingDir + "/table").Columns({"bar", "foo"}) });
    const auto& statistics = statisticsList.front();

    EXPECT_EQ(std::ssize(statistics.ColumnDataWeight), 2);
    EXPECT_TRUE(statistics.ColumnDataWeight.at("foo") > 0);
    EXPECT_TRUE(statistics.ColumnDataWeight.at("bar") > 0);

    // Set very long retry interval to ensure that any retries will hang the thread.
    TConfig::Get()->RetryInterval = TDuration::Seconds(100);
    TConfig::Get()->UseAbortableResponse = true;
    auto threadPool = CreateThreadPool(2);
    auto done = NThreading::Async(
        [&] {
            // Table doesn't exist in the root transaction.
            EXPECT_THROW(
                client->GetTableColumnarStatistics({ TRichYPath(workingDir + "/table").Columns({"bar", "foo"}) }),
                TErrorResponse);
        },
        *threadPool);
    auto start = TInstant::Now();
    Sleep(TDuration::Seconds(1));
    TAbortableHttpResponse::AbortAll("/get_table_columnar_statistics");
    EXPECT_NO_THROW(done.GetValueSync());
    EXPECT_TRUE(TInstant::Now() - start < TDuration::Seconds(10));
}

TEST(CypressClient, TestGetTablePartitions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();
    {
        auto writer1 = tx->CreateTableWriter<TNode>(workingDir + "/table1");
        writer1->AddRow(TNode()("foo", 1)("bar", "baz"));
        writer1->Finish();

        auto writer2 = tx->CreateTableWriter<TNode>(workingDir + "/table2");
        writer2->AddRow(TNode()("foo", 2)("bar", "qux"));
        writer2->Finish();
    }

    auto options = NYT::TGetTablePartitionsOptions()
        .PartitionMode(NYT::ETablePartitionMode::Unordered)
        .DataWeightPerPartition(100'000);

    auto partitions = tx->GetTablePartitions(
        {TRichYPath(workingDir + "/table1"), TRichYPath(workingDir + "/table2")},
        options);

    EXPECT_TRUE(partitions.Partitions.size() > 0);
    EXPECT_TRUE(partitions.Partitions[0].TableRanges.size() > 0);
}

TEST(CypressClient, TestConcurrency) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    client->Set(workingDir + "/foo", 54);

    auto threadPool = CreateThreadPool(20);

    const auto writer = [&] {
        for (int i = 0; i != 500; ++i) {
            client->Set(workingDir + "/foo", 42);
        }
    };

    const auto reader = [&] {
        for (int i = 0; i != 500; ++i) {
            client->Get(workingDir + "/foo");
        }
    };

    TVector<NThreading::TFuture<void>> results;
    for (int i = 0; i != 10; ++i) {
        results.emplace_back(NThreading::Async(MakeCopy(writer), *threadPool));
    };
    for (int i = 0; i != 10; ++i) {
        results.emplace_back(NThreading::Async(MakeCopy(reader), *threadPool));
    };

    for (auto& f : results) {
        f.Wait();
    }
}

TEST(CypressClient, FileCache)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TYPath cachePath = workingDir + "/file_cache";
    client->Create(cachePath, ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));

    {
        TString content = "Hello world!";
        {
            auto writer = client->CreateFileWriter(workingDir + "/file", TFileWriterOptions().ComputeMD5(true));
            *writer << content;
            writer->Finish();
        }
        auto expirationTimeout = 600000;
        client->Set(workingDir + "/file/@expiration_timeout", expirationTimeout);
        auto md5 = MD5::Calc(content);
        auto pathInCache = client->PutFileToCache(
            workingDir + "/file",
            md5,
            cachePath,
            TPutFileToCacheOptions().PreserveExpirationTimeout(true));

        auto maybePath = client->GetFileFromCache(md5, cachePath);
        EXPECT_TRUE(maybePath.Defined());
        EXPECT_EQ(expirationTimeout, client->Get(*maybePath + "/@expiration_timeout"));
        EXPECT_EQ(content, client->CreateFileReader(*maybePath)->ReadAll());
    }

    {
        auto tx = client->StartTransaction();

        TString content = "Hello world again!";
        {
            auto writer = tx->CreateFileWriter(workingDir + "/file2", TFileWriterOptions().ComputeMD5(true));
            *writer << content;
            writer->Finish();
        }

        auto md5 = MD5::Calc(content);
        auto pathInCache = tx->PutFileToCache(workingDir + "/file2", md5, cachePath);

        auto maybePath = tx->GetFileFromCache(md5, cachePath);
        EXPECT_TRUE(maybePath.Defined());
        EXPECT_EQ(content, tx->CreateFileReader(*maybePath)->ReadAll());

        maybePath = client->GetFileFromCache(md5, cachePath);
        EXPECT_TRUE(!maybePath.Defined());

        tx->Commit();

        maybePath = client->GetFileFromCache(md5, cachePath);
        EXPECT_TRUE(maybePath.Defined());
        EXPECT_EQ(content, client->CreateFileReader(*maybePath)->ReadAll());
    }
}

TEST(CypressClient, AbortableHttpResponse)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TConfig::Get()->UseAbortableResponse = true;
    TConfig::Get()->RetryInterval = TDuration();
    TConfig::Get()->ReadRetryCount = 10;

    auto size = 1000;
    auto data = GenerateRandomData(size);
    client->Set(workingDir + "/x", data);
    {
        auto outage = TAbortableHttpResponse::StartOutage(
            "/get",
            TOutageOptions().LengthLimit(100));
        EXPECT_THROW(client->Get(workingDir + "/x"), TAbortedForTestPurpose);
    }
    {
        auto outage = TAbortableHttpResponse::StartOutage(
            "/get",
            TOutageOptions().LengthLimit(100).ResponseCount(8));
        EXPECT_EQ(client->Get(workingDir + "/x").AsString(), data);
    }
    {
        auto outage = TAbortableHttpResponse::StartOutage(
            "/get",
            TOutageOptions().LengthLimit(10000));
        EXPECT_EQ(client->Get(workingDir + "/x").AsString(), data);
    }
}

TEST(CypressClient, TestMultisetAttributes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    client->Create(workingDir + "/node", NT_MAP);

    auto tx = client->StartTransaction();

    tx->MultisetAttributes(workingDir + "/node/@", {
        {"a", TNode("foo")},
        {"b", TNode()("c", "bar")},
    });
    EXPECT_EQ(tx->Get(workingDir + "/node/@a"), TNode("foo"));
    EXPECT_EQ(tx->Get(workingDir + "/node/@b/c"), TNode("bar"));
    EXPECT_EQ(tx->Exists(workingDir + "/node/@a"), true);
    EXPECT_EQ(client->Exists(workingDir + "/node/@a"), false);

    tx->MultisetAttributes(workingDir + "/node/@", {
        {"b", TNode()("d", "qux")},
    });
    EXPECT_EQ(tx->Get(workingDir + "/node/@a"), TNode("foo"));
    EXPECT_EQ(tx->Get(workingDir + "/node/@b/d"), TNode("qux"));
}
