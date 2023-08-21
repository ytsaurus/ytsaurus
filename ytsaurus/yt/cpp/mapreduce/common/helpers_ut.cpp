#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/json/json_reader.h>
#include <yt/cpp/mapreduce/common/helpers.h>

using namespace NJson;
using namespace NYT;

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
        s << "TNode:" << NodeToYsonString(node);
}

static TJsonValue ReadJson(TStringBuf input)
{
    TMemoryInput in(input);
    TJsonValue result;
    ReadJsonTree(&in, &result, /*throwOnError=*/true);
    return result;
}

Y_UNIT_TEST_SUITE(NodeFromJson)
{
    Y_UNIT_TEST(TestSimpleMap) {
        const char* input =
            R"""( { )"""
            R"""(   "foo": "bar" )"""
            R"""( } )"""
            ;

        const TNode resultText = NodeFromJsonString(input);
        UNIT_ASSERT_VALUES_EQUAL(resultText, TNode()("foo", "bar"));

        const TNode resultNode = NodeFromJsonValue(ReadJson(input));
        UNIT_ASSERT_VALUES_EQUAL(resultNode, TNode()("foo", "bar"));
    }

    Y_UNIT_TEST(TestSimpleArray) {
        const char* input =
            R"""( [ )"""
            R"""(  { )"""
            R"""(   "foo": "bar" )"""
            R"""(  }, )"""
            R"""(  123, )"""
            R"""(  [4, 2, {}], )"""
            R"""(  false )"""
            R"""( ] )"""
            ;

        const TNode expected =
            TNode()
                .Add(TNode()("foo", "bar"))
                .Add(123)
                .Add(TNode().Add(4).Add(2).Add(TNode::CreateMap()))
                .Add(false);

        const TNode resultText = NodeFromJsonString(input);
        UNIT_ASSERT_VALUES_EQUAL(resultText, expected);
        const TNode resultNode = NodeFromJsonValue(ReadJson(input));
        UNIT_ASSERT_VALUES_EQUAL(resultNode, expected);
    }
}
