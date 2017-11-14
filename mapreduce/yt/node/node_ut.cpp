#include "node.h"
#include "node_io.h"

#include <library/unittest/registar.h>

#include <util/ysaveload.h>

using namespace NYT;

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToYsonString(node);
}

SIMPLE_UNIT_TEST_SUITE(YtNodeTest) {
    SIMPLE_UNIT_TEST(TestConstsructors) {
        TNode nodeEmpty;
        UNIT_ASSERT_EQUAL(nodeEmpty.GetType(), TNode::Undefined);

        TNode nodeString("foobar");
        UNIT_ASSERT_EQUAL(nodeString.GetType(), TNode::String);
        UNIT_ASSERT(nodeString.IsString());
        UNIT_ASSERT_VALUES_EQUAL(nodeString.AsString(), "foobar");

        TNode nodeInt(int(54));
        UNIT_ASSERT_EQUAL(nodeInt.GetType(), TNode::Int64);
        UNIT_ASSERT(nodeInt.IsInt64());
        UNIT_ASSERT(!nodeInt.IsUint64());
        UNIT_ASSERT_VALUES_EQUAL(nodeInt.AsInt64(), 54ull);

        TNode nodeUint(ui64(42));
        UNIT_ASSERT_EQUAL(nodeUint.GetType(), TNode::Uint64);
        UNIT_ASSERT(nodeUint.IsUint64());
        UNIT_ASSERT(!nodeUint.IsInt64());
        UNIT_ASSERT_VALUES_EQUAL(nodeUint.AsUint64(), 42ull);

        TNode nodeDouble(double(2.3));
        UNIT_ASSERT_EQUAL(nodeDouble.GetType(), TNode::Double);
        UNIT_ASSERT(nodeDouble.IsDouble());
        UNIT_ASSERT_VALUES_EQUAL(nodeDouble.AsDouble(), double(2.3));

        TNode nodeBool(true);
        UNIT_ASSERT_EQUAL(nodeBool.GetType(), TNode::Bool);
        UNIT_ASSERT(nodeBool.IsBool());
        UNIT_ASSERT_VALUES_EQUAL(nodeBool.AsBool(), true);

        TNode nodeEntity = TNode::CreateEntity();
        UNIT_ASSERT_EQUAL(nodeEntity.GetType(), TNode::Null);
        UNIT_ASSERT(nodeEntity.IsEntity());
    }

    SIMPLE_UNIT_TEST(TestNodeMap) {
        TNode nodeMap = TNode()("foo", "bar")("bar", "baz");
        UNIT_ASSERT(nodeMap.IsMap());
        UNIT_ASSERT_EQUAL(nodeMap.GetType(), TNode::Map);
        UNIT_ASSERT_VALUES_EQUAL(nodeMap.Size(), 2);

        UNIT_ASSERT(nodeMap.HasKey("foo"));
        UNIT_ASSERT(!nodeMap.HasKey("42"));
        UNIT_ASSERT_EQUAL(nodeMap["foo"], TNode("bar"));
        UNIT_ASSERT_EQUAL(nodeMap["bar"], TNode("baz"));

        // const version of operator[]
        UNIT_ASSERT_EQUAL(static_cast<const TNode&>(nodeMap)["42"].GetType(), TNode::Undefined);
        UNIT_ASSERT(!nodeMap.HasKey("42"));

        // nonconst version of operator[]
        UNIT_ASSERT_EQUAL(nodeMap["42"].GetType(), TNode::Undefined);
        UNIT_ASSERT(nodeMap.HasKey("42"));

        nodeMap("rock!!!", TNode()
            ("Pink", "Floyd")
            ("Purple", "Deep"));

        TNode copyNode;
        copyNode = nodeMap;
        UNIT_ASSERT_EQUAL(copyNode["foo"], TNode("bar"));
        UNIT_ASSERT_EQUAL(copyNode["bar"], TNode("baz"));
        UNIT_ASSERT(copyNode["42"].GetType() == TNode::Undefined);
        UNIT_ASSERT_EQUAL(copyNode["rock!!!"]["Purple"], TNode("Deep"));
    }

    SIMPLE_UNIT_TEST(TestNodeList) {
        TNode nodeList = TNode().Add("foo").Add(42).Add(3.14);
        UNIT_ASSERT(nodeList.IsList());
        UNIT_ASSERT_EQUAL(nodeList.GetType(), TNode::List);
        UNIT_ASSERT_VALUES_EQUAL(nodeList.Size(), 3);

        UNIT_ASSERT_EQUAL(nodeList[1], TNode(42));
        nodeList.Add(TNode().Add("ls").Add("pwd"));

        TNode copyNode;
        copyNode = nodeList;
        UNIT_ASSERT_EQUAL(copyNode[0], TNode("foo"));
        UNIT_ASSERT_EQUAL(copyNode[3][1], TNode("pwd"));
    }

    SIMPLE_UNIT_TEST(TestInsertingMethodsFromTemporaryObjects) {
        // check that .Add(...) doesn't return lvalue reference to temporary object
        {
            const TNode& nodeList = TNode().Add(0).Add("pass").Add(0);
            UNIT_ASSERT_EQUAL(nodeList[1], TNode("pass"));
        }

        // check that .operator()(...) doesn't return lvalue reference to temporary object
        {
            const TNode& nodeMap = TNode()("1", 0)("2", "pass")("3", 0);
            UNIT_ASSERT_EQUAL(nodeMap["2"], TNode("pass"));
        }
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

    SIMPLE_UNIT_TEST(TestSaveLoad) {
        TNode node = TNode()("foo", "bar")("baz", 42);
        node.Attributes()["attr_name"] = "attr_value";

        TString bytes;
        {
            TStringOutput s(bytes);
            ::Save(&s, node);
        }

        TNode nodeCopy;
        {
            TStringInput s(bytes);
            ::Load(&s, nodeCopy);
        }

        UNIT_ASSERT_VALUES_EQUAL(node, nodeCopy);
    }

    SIMPLE_UNIT_TEST(TestIntCast) {
        TNode node = 1ull << 31;
        UNIT_ASSERT(node.IsUint64());
        UNIT_ASSERT(node.IntCast<ui64>() == node.AsUint64());
        UNIT_ASSERT(node.IntCast<i64>() == static_cast<i64>(node.AsUint64()));
        node = 1ull << 63;
        UNIT_ASSERT(node.IsUint64());
        UNIT_ASSERT_EXCEPTION(node.IntCast<i64>(), TNode::TTypeError);
        UNIT_ASSERT(node.IntCast<ui64>() == node.AsUint64());

        node = 12345;
        UNIT_ASSERT(node.IsInt64());
        UNIT_ASSERT(node.IntCast<i64>() == node.AsInt64());
        UNIT_ASSERT(node.IntCast<ui64>() == static_cast<ui64>(node.AsInt64()));
        node = -5;
        UNIT_ASSERT(node.IsInt64());
        UNIT_ASSERT(node.IntCast<i64>() == node.AsInt64());
        UNIT_ASSERT_EXCEPTION(node.IntCast<ui64>(), TNode::TTypeError);
    }

    SIMPLE_UNIT_TEST(TestConvertToString) {
        UNIT_ASSERT_VALUES_EQUAL(TNode(5).ConvertTo<TString>(), "5");
        UNIT_ASSERT_VALUES_EQUAL(TNode(123432423).ConvertTo<TString>(), "123432423");
        UNIT_ASSERT_VALUES_EQUAL(TNode(123456789012345678ll).ConvertTo<TString>(), "123456789012345678");
        UNIT_ASSERT_VALUES_EQUAL(TNode(123456789012345678ull).ConvertTo<TString>(), "123456789012345678");
        UNIT_ASSERT_VALUES_EQUAL(TNode(-123456789012345678ll).ConvertTo<TString>(), "-123456789012345678");
        UNIT_ASSERT_VALUES_EQUAL(TNode(true).ConvertTo<TString>(), "1");
        UNIT_ASSERT_VALUES_EQUAL(TNode(false).ConvertTo<TString>(), "0");
        UNIT_ASSERT_VALUES_EQUAL(TNode(5.3).ConvertTo<TString>(), "5.3");
    }

    SIMPLE_UNIT_TEST(TestConvertFromString) {
        UNIT_ASSERT_VALUES_EQUAL(TNode("123456789012345678").ConvertTo<ui64>(), 123456789012345678ull);
        UNIT_ASSERT_VALUES_EQUAL(TNode("123456789012345678").ConvertTo<i64>(), 123456789012345678);
        UNIT_ASSERT_VALUES_EQUAL(TNode(ToString(1ull << 63)).ConvertTo<ui64>(), 1ull << 63);
        UNIT_ASSERT_EXCEPTION(TNode(ToString(1ull << 63)).ConvertTo<i64>(), TFromStringException);
        UNIT_ASSERT_VALUES_EQUAL(TNode("5.34").ConvertTo<double>(), 5.34);
    }

    SIMPLE_UNIT_TEST(TestConvertDoubleInt) {
        UNIT_ASSERT_VALUES_EQUAL(TNode(5.3).ConvertTo<ui64>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(TNode(5.3).ConvertTo<i64>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(TNode(-5.3).ConvertTo<i64>(), -5);
        UNIT_ASSERT_EXCEPTION(TNode(-5.3).ConvertTo<ui64>(), TNode::TTypeError);
        UNIT_ASSERT_EXCEPTION(TNode(1e100).ConvertTo<i64>(), TNode::TTypeError);
        UNIT_ASSERT_EXCEPTION(TNode(1e100).ConvertTo<ui64>(), TNode::TTypeError);
        {
            double v = (1ull << 63) + (1ull);
            TNode node = v;
            UNIT_ASSERT(node.IsDouble());
            UNIT_ASSERT_EXCEPTION(node.ConvertTo<i64>(), TNode::TTypeError);
            UNIT_ASSERT_VALUES_EQUAL(node.ConvertTo<ui64>(), static_cast<ui64>(v));
        }
        {
            double v = (double)(1ull << 63) + (1ull << 63);
            TNode node = v;
            UNIT_ASSERT(node.IsDouble());
            UNIT_ASSERT_EXCEPTION(node.ConvertTo<i64>(), TNode::TTypeError);
            UNIT_ASSERT_EXCEPTION(node.ConvertTo<ui64>(), TNode::TTypeError);
        }
        UNIT_ASSERT_EXCEPTION(TNode(NAN).ConvertTo<ui64>(), TNode::TTypeError);
        UNIT_ASSERT_EXCEPTION(TNode(NAN).ConvertTo<i64>(), TNode::TTypeError);

        UNIT_ASSERT_EXCEPTION(TNode(INFINITY).ConvertTo<ui64>(), TNode::TTypeError);
        UNIT_ASSERT_EXCEPTION(TNode(INFINITY).ConvertTo<i64>(), TNode::TTypeError);
    }

    SIMPLE_UNIT_TEST(TestConvertToBool) {
        UNIT_ASSERT_VALUES_EQUAL(TNode("true").ConvertTo<bool>(), true);
        UNIT_ASSERT_VALUES_EQUAL(TNode("TRUE").ConvertTo<bool>(), true);
        UNIT_ASSERT_VALUES_EQUAL(TNode("false").ConvertTo<bool>(), false);
        UNIT_ASSERT_VALUES_EQUAL(TNode("FALSE").ConvertTo<bool>(), false);
        UNIT_ASSERT_VALUES_EQUAL(TNode(1).ConvertTo<bool>(), true);
        UNIT_ASSERT_VALUES_EQUAL(TNode(0).ConvertTo<bool>(), false);
        UNIT_ASSERT_EXCEPTION(TNode("random").ConvertTo<bool>(), TFromStringException);
        UNIT_ASSERT_EXCEPTION(TNode("").ConvertTo<bool>(), TFromStringException);
    }

    SIMPLE_UNIT_TEST(TestETypeToString) {
        // Compatibility
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::UNDEFINED), "undefined");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::STRING), "string_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::INT64), "int64_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::UINT64), "uint64_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::DOUBLE), "double_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::BOOL), "boolean_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::LIST), "list_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::MAP), "map_node");
        UNIT_ASSERT_VALUES_EQUAL(ToString(TNode::ENTITY), "null");
    }
}
