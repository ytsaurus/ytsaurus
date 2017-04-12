#include "lib.h"

#include <mapreduce/yt/client/rpc_parameters_serialization.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

#include <util/generic/guid.h>

using namespace NYT;
using namespace NYT::NTesting;

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
            auto nodeTypeStr = NYT::NDetail::ToString(nodeType);
            const Stroka nodePath = "//testing/" + nodeTypeStr;
            const Stroka nodeTypePath = nodePath + "/@type";
            const Stroka nodeIdPath = nodePath + "/@id";

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
}
