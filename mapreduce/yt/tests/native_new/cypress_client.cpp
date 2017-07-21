#include "lib.h"

#include <mapreduce/yt/client/rpc_parameters_serialization.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>

using namespace NYT;
using namespace NYT::NTesting;

static TNode::TList SortedStrings(TNode::TList input) {
    std::sort(input.begin(), input.end(), [] (const TNode& lhs, const TNode& rhs) {
        return lhs.AsString() < rhs.AsString();
    });
    return input;
}

SIMPLE_UNIT_TEST_SUITE(CypressClient) {
    SIMPLE_UNIT_TEST(TestCreateAllTypes)
    {
        auto client = CreateTestClient();

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
            auto nodeTypeStr = ::ToString(nodeType);
            const TString nodePath = "//testing/" + nodeTypeStr;
            const TString nodeTypePath = nodePath + "/@type";
            const TString nodeIdPath = nodePath + "/@id";

            auto nodeId = client->Create(nodePath, nodeType);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(nodeTypePath), nodeTypeStr);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(nodeIdPath), GetGuidAsString(nodeId));
        }
    }

    SIMPLE_UNIT_TEST(TestCreate)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Create("//testing/map_node", NT_MAP);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/map_node"), true);

        tx->Create("//testing/tx_map_node", NT_MAP);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/tx_map_node"), false);
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/tx_map_node"), true);

        UNIT_ASSERT_EXCEPTION(
            client->Create("//testing/recursive_not_set_dir/node", NT_TABLE),
            TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/recursive_not_set_dir"), false);

        client->Create("//testing/recursive_set_dir/node", NT_TABLE, TCreateOptions().Recursive(true));
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/recursive_set_dir"), true);

        client->Create("//testing/existing_table", NT_TABLE);
        UNIT_ASSERT_EXCEPTION(
            client->Create("//testing/existing_table", NT_TABLE),
            TErrorResponse);
        client->Create("//testing/existing_table", NT_TABLE, TCreateOptions().IgnoreExisting(true));
        UNIT_ASSERT_EXCEPTION(
            client->Create("//testing/existing_table", NT_MAP, TCreateOptions().IgnoreExisting(true)),
            TErrorResponse);

        client->Create("//testing/node_with_attributes", NT_TABLE, TCreateOptions().Attributes(TNode()("attr_name", "attr_value")));
        UNIT_ASSERT_VALUES_EQUAL(
            client->Get("//testing/node_with_attributes/@attr_name"),
            TNode("attr_value"));

        {
            auto initialNodeId = client->Create("//testing/existing_table_for_force", NT_TABLE);

            auto nonForceNodeId = client->Create("//testing/existing_table_for_force", NT_TABLE, TCreateOptions().IgnoreExisting(true));
            UNIT_ASSERT_VALUES_EQUAL(initialNodeId, nonForceNodeId);
            auto forceNodeId = client->Create("//testing/existing_table_for_force", NT_TABLE, TCreateOptions().Force(true));
            UNIT_ASSERT(forceNodeId != initialNodeId);
        }
    }

    SIMPLE_UNIT_TEST(TestRemove)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Create("//testing/table", NT_TABLE);
        client->Remove("//testing/table");
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/table"), false);

        tx->Create("//testing/tx_table", NT_TABLE);
        tx->Remove("//testing/tx_table");
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/tx_table"), false);

        client->Create("//testing/map_node/table_node", NT_TABLE, TCreateOptions().Recursive(true));

        UNIT_ASSERT_EXCEPTION(
            client->Remove("//testing/map_node"),
            TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/map_node/table_node"), true);
        client->Remove("//testing/map_node", TRemoveOptions().Recursive(true));
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/map_node"), false);

        UNIT_ASSERT_EXCEPTION(
            client->Remove("//testing/missing_node"),
            TErrorResponse);
        client->Remove("//testing/missing_node", TRemoveOptions().Force(true));
    }

    SIMPLE_UNIT_TEST(TestSetGet)
    {
        auto client = CreateTestClient();
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
            client->Remove("//testing/node", TRemoveOptions().Recursive(true).Force(true));
            client->Set("//testing/node", node);
            UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/node"), node);
        }


        auto tx = client->StartTransaction();
        tx->Set("//testing/tx_node", TNode(10050));
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/tx_node"), false);
        UNIT_ASSERT_VALUES_EQUAL(tx->Get("//testing/tx_node"), TNode(10050));

        client->Create("//testing/node_with_attr", NT_TABLE);
        client->Set("//testing/node_with_attr/@attr_name", TNode("attr_value"));

        auto nodeWithAttr = client->Get("//testing/node_with_attr",
            TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("attr_name")));

        UNIT_ASSERT_VALUES_EQUAL(nodeWithAttr.GetAttributes().AsMap().at("attr_name"), TNode("attr_value"));
    }

    SIMPLE_UNIT_TEST(TestList)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();
        client->Set("//testing/foo", 5);
        client->Set("//testing/bar", "bar");
        client->Set("//testing/bar", "bar");
        client->Set("//testing/bar/@attr_name", "attr_value");
        tx->Set("//testing/tx_qux", "gg");

        auto res = client->List("//testing");

        UNIT_ASSERT_VALUES_EQUAL(
            SortedStrings(res),
            TNode::TList({"bar", "foo"}));

        auto txRes = tx->List("//testing");
        UNIT_ASSERT_VALUES_EQUAL(
            SortedStrings(txRes),
            TNode::TList({"bar", "foo", "tx_qux"}));

        auto maxSizeRes = client->List("//testing", TListOptions().MaxSize(1));
        UNIT_ASSERT_VALUES_EQUAL(maxSizeRes.size(), 1);
        UNIT_ASSERT(yhash_set<TString>({"foo", "bar"}).has(maxSizeRes[0].AsString()));

        auto attrFilterRes = client->List("//testing",
            TListOptions().AttributeFilter(TAttributeFilter().AddAttribute("attr_name")));
        attrFilterRes = SortedStrings(attrFilterRes);
        auto barNode = TNode("bar");
        barNode.Attributes()("attr_name", "attr_value");
        UNIT_ASSERT_VALUES_EQUAL(
            attrFilterRes,
            TNode::TList({barNode, "foo"}));
    }

    SIMPLE_UNIT_TEST(TestCopy)
    {
        auto client = CreateTestClient();

        client->Set("//testing/simple", "simple value");
        client->Copy("//testing/simple", "//testing/copy_simple");
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/copy_simple"), client->Get("//testing/simple"));
    }

    SIMPLE_UNIT_TEST(TestMove)
    {
        auto client = CreateTestClient();

        client->Set("//testing/simple", "simple value");
        auto oldValue = client->Get("//testing/simple");
        client->Move("//testing/simple", "//testing/moved_simple");
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/moved_simple"), oldValue);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/simple"), false);
    }

    SIMPLE_UNIT_TEST(TestCopy_PreserveExpirationTime)
    {
        auto client = CreateTestClient();

        const TString expirationTime = "2042-02-15T18:45:19.591902Z";
        for (TString path : {"//testing/table_default", "//testing/table_false", "//testing/table_true"}) {
            client->Create(path, NT_TABLE);
            client->Set(path + "/@expiration_time", expirationTime);
        }

        client->Copy("//testing/table_default", "//testing/copy_table_default");
        client->Copy("//testing/table_true", "//testing/copy_table_true", TCopyOptions().PreserveExpirationTime(true));
        client->Copy("//testing/table_false", "//testing/copy_table_false", TCopyOptions().PreserveExpirationTime(false));

        UNIT_ASSERT_EXCEPTION(client->Get("//testing/copy_table_default/@expiration_time"), yexception);
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/copy_table_true/@expiration_time"), expirationTime);
        UNIT_ASSERT_EXCEPTION(client->Get("//testing/copy_table_false/@expiration_time"), yexception);
    }

    SIMPLE_UNIT_TEST(TestMove_PreserveExpirationTime)
    {
        auto client = CreateTestClient();

        const TString expirationTime = "2042-02-15T18:45:19.591902Z";
        for (TString path : {"//testing/table_default", "//testing/table_false", "//testing/table_true"}) {
            client->Create(path, NT_TABLE);
            client->Set(path + "/@expiration_time", expirationTime);
        }

        client->Move("//testing/table_default", "//testing/moved_table_default");
        client->Move("//testing/table_true", "//testing/moved_table_true", TMoveOptions().PreserveExpirationTime(true));
        client->Move("//testing/table_false", "//testing/moved_table_false", TMoveOptions().PreserveExpirationTime(false));

        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/moved_table_default/@expiration_time"), TNode(expirationTime));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/moved_table_true/@expiration_time"), TNode(expirationTime));
        UNIT_ASSERT_EXCEPTION(client->Get("//testing/moved_table_false/@expiration_time"), yexception);
    }

    SIMPLE_UNIT_TEST(TestLink)
    {
        auto client = CreateTestClient();

        client->Create("//testing/table", NT_TABLE);
        client->Link("//testing/table", "//testing/table_link");

        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/table"), true);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/table_link"), true);
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/table_link&/@target_path"), "//testing/table");
    }

    SIMPLE_UNIT_TEST(TestConcatenate)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateFileWriter("//testing/file1");
            *writer << "foo";
            writer->Finish();
        }
        {
            auto writer = client->CreateFileWriter("//testing/file2");
            *writer << "bar";
            writer->Finish();
        }
        client->Create("//testing/concat", NT_FILE);
        yvector<TYPath> nodes{"//testing/file1", "//testing/file2"};
        client->Concatenate(nodes, "//testing/concat");
        {
            auto reader = client->CreateFileReader("//testing/concat");
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), "foobar");
        }
        client->Concatenate(nodes, "//testing/concat", TConcatenateOptions().Append(true));
        {
            auto reader = client->CreateFileReader("//testing/concat");
            UNIT_ASSERT_VALUES_EQUAL(reader->ReadAll(), "foobarfoobar");
        }
    }
}
