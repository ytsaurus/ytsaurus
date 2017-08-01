#include "node.h"

#include <library/unittest/registar.h>

using namespace NYT;

SIMPLE_UNIT_TEST_SUITE(YtNodeTest) {
    SIMPLE_UNIT_TEST(TestConstsructors) {
        TNode nodeEmpty;
        UNIT_ASSERT_EQUAL(nodeEmpty.GetType(), TNode::UNDEFINED);

        TNode nodeString("foobar");
        UNIT_ASSERT_EQUAL(nodeString.GetType(), TNode::STRING);
        UNIT_ASSERT(nodeString.IsString());
        UNIT_ASSERT_VALUES_EQUAL(nodeString.AsString(), "foobar");

        TNode nodeInt(int(54));
        UNIT_ASSERT_EQUAL(nodeInt.GetType(), TNode::INT64);
        UNIT_ASSERT(nodeInt.IsInt64());
        UNIT_ASSERT(!nodeInt.IsUint64());
        UNIT_ASSERT_VALUES_EQUAL(nodeInt.AsInt64(), 54ull);

        TNode nodeUint(ui64(42));
        UNIT_ASSERT_EQUAL(nodeUint.GetType(), TNode::UINT64);
        UNIT_ASSERT(nodeUint.IsUint64());
        UNIT_ASSERT(!nodeUint.IsInt64());
        UNIT_ASSERT_VALUES_EQUAL(nodeUint.AsUint64(), 42ull);

        TNode nodeDouble(double(2.3));
        UNIT_ASSERT_EQUAL(nodeDouble.GetType(), TNode::DOUBLE);
        UNIT_ASSERT(nodeDouble.IsDouble());
        UNIT_ASSERT_VALUES_EQUAL(nodeDouble.AsDouble(), double(2.3));

        TNode nodeBool(true);
        UNIT_ASSERT_EQUAL(nodeBool.GetType(), TNode::BOOL);
        UNIT_ASSERT(nodeBool.IsBool());
        UNIT_ASSERT_VALUES_EQUAL(nodeBool.AsBool(), true);

        TNode nodeEntity = TNode::CreateEntity();
        UNIT_ASSERT_EQUAL(nodeEntity.GetType(), TNode::ENTITY);
        UNIT_ASSERT(nodeEntity.IsEntity());
    }

    SIMPLE_UNIT_TEST(TestNodeMap) {
        TNode nodeMap = TNode()("foo", "bar")("bar", "baz");
        UNIT_ASSERT(nodeMap.IsMap());
        UNIT_ASSERT_EQUAL(nodeMap.GetType(), TNode::MAP);
        UNIT_ASSERT_VALUES_EQUAL(nodeMap.Size(), 2);

        UNIT_ASSERT(nodeMap.HasKey("foo"));
        UNIT_ASSERT(!nodeMap.HasKey("42"));
        UNIT_ASSERT_EQUAL(nodeMap["foo"], TNode("bar"));
        UNIT_ASSERT_EQUAL(nodeMap["bar"], TNode("baz"));

        // const version of operator[]
        UNIT_ASSERT_EQUAL(static_cast<const TNode&>(nodeMap)["42"].GetType(), TNode::UNDEFINED);
        UNIT_ASSERT(!nodeMap.HasKey("42"));

        // nonconst version of operator[]
        UNIT_ASSERT_EQUAL(nodeMap["42"].GetType(), TNode::UNDEFINED);
        UNIT_ASSERT(nodeMap.HasKey("42"));

        nodeMap("rock!!!", TNode()
            ("Pink", "Floyd")
            ("Purple", "Deep"));

        TNode copyNode;
        copyNode = nodeMap;
        UNIT_ASSERT_EQUAL(copyNode["foo"], TNode("bar"));
        UNIT_ASSERT_EQUAL(copyNode["bar"], TNode("baz"));
        UNIT_ASSERT(copyNode["42"].GetType() == TNode::UNDEFINED);
        UNIT_ASSERT_EQUAL(copyNode["rock!!!"]["Purple"], TNode("Deep"));
    }

    SIMPLE_UNIT_TEST(TestNodeList) {
        TNode nodeList = TNode().Add("foo").Add(42).Add(3.14);
        UNIT_ASSERT(nodeList.IsList());
        UNIT_ASSERT_EQUAL(nodeList.GetType(), TNode::LIST);
        UNIT_ASSERT_VALUES_EQUAL(nodeList.Size(), 3);

        UNIT_ASSERT_EQUAL(nodeList[1], TNode(42));
        nodeList.Add(TNode().Add("ls").Add("pwd"));

        TNode copyNode;
        copyNode = nodeList;
        UNIT_ASSERT_EQUAL(copyNode[0], TNode("foo"));
        UNIT_ASSERT_EQUAL(copyNode[3][1], TNode("pwd"));
    }

    SIMPLE_UNIT_TEST(TestAttributes) {
        TNode node = TNode()("lee", 42)("faa", 54);
        UNIT_ASSERT(!node.HasAttributes());
        node.Attributes()("foo", true)("bar", false);
        UNIT_ASSERT(node.HasAttributes());

        {
            TNode copyNode;
            UNIT_ASSERT(!copyNode.HasAttributes());
            copyNode = node;
            UNIT_ASSERT(copyNode.HasAttributes());
            UNIT_ASSERT_EQUAL(copyNode.GetAttributes()["foo"], TNode(true));
        }

        {
            TNode movedWithoutAttributes(42);
            movedWithoutAttributes.Attributes()("one", 1)("two", 2);
            movedWithoutAttributes.MoveWithoutAttributes(TNode(node));
            UNIT_ASSERT(movedWithoutAttributes.IsMap());
            UNIT_ASSERT_EQUAL(movedWithoutAttributes["lee"], TNode(42));
            UNIT_ASSERT_EQUAL(movedWithoutAttributes.GetAttributes()["one"], TNode(1));
            UNIT_ASSERT(!movedWithoutAttributes.GetAttributes().HasKey("foo"));
        }

        {
            TNode copyNode = node;
            UNIT_ASSERT(copyNode.HasAttributes());
            UNIT_ASSERT(copyNode.GetAttributes().HasKey("foo"));
            copyNode.ClearAttributes();
            UNIT_ASSERT(!copyNode.HasAttributes());
            UNIT_ASSERT(!copyNode.GetAttributes().HasKey("foo"));
        }

        {
            TNode copyNode = node;
            UNIT_ASSERT(copyNode.HasAttributes());
            UNIT_ASSERT(copyNode.GetAttributes().HasKey("foo"));
            copyNode.Clear();
            UNIT_ASSERT(!copyNode.HasAttributes());
            UNIT_ASSERT(!copyNode.GetAttributes().HasKey("foo"));
        }
    }

    SIMPLE_UNIT_TEST(TestEq) {
        TNode nodeNoAttributes = TNode()("lee", 42)("faa", 54);
        TNode node = nodeNoAttributes;
        node.Attributes()("foo", true)("bar", false);
        UNIT_ASSERT(node != nodeNoAttributes);
        UNIT_ASSERT(nodeNoAttributes != node);
        TNode copyNode = node;
        UNIT_ASSERT(copyNode == node);
        UNIT_ASSERT(node == copyNode);
    }
}
