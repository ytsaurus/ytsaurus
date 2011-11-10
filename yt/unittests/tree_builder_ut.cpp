#include "../ytlib/ytree/tree_builder.h"
#include "../ytlib/ytree/tree_visitor.h"
#include "../ytlib/ytree/yson_writer.h"
#include "../ytlib/ytree/ephemeral.h"

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TMockConsumer
    : public NYTree::IYsonConsumer
{
public:
    MOCK_METHOD2(OnStringScalar, void(const Stroka& value, bool hasAttributes));
    MOCK_METHOD2(OnInt64Scalar, void(i64 value, bool hasAttributes));
    MOCK_METHOD2(OnDoubleScalar, void(double value, bool hasAttributes));
    MOCK_METHOD1(OnEntity, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginList, void());
    MOCK_METHOD0(OnListItem, void());
    MOCK_METHOD1(OnEndList, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginMap, void());
    MOCK_METHOD1(OnMapItem, void(const Stroka& name));
    MOCK_METHOD1(OnEndMap, void(bool hasAttributes));

    MOCK_METHOD0(OnBeginAttributes, void());
    MOCK_METHOD1(OnAttributesItem, void(const Stroka& name));
    MOCK_METHOD0(OnEndAttributes, void());
};

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilderTest: public ::testing::Test
{
public:
    StrictMock<TMockConsumer> Mock;
};


////////////////////////////////////////////////////////////////////////////////

TEST_F(TTreeBuilderTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap(false));

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnEndMap(false);
    auto root = builder->EndTree();

    TTreeVisitor visitor(&Mock);
    visitor.Visit(root);
}

TEST_F(TTreeBuilderTest, NestedMaps)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnMapItem("a"));
        EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnMapItem("b"));
            EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnMapItem("c"));
            EXPECT_CALL(Mock, OnInt64Scalar(42, false));
            EXPECT_CALL(Mock, OnEndMap(false));
        EXPECT_CALL(Mock, OnEndMap(false));
    EXPECT_CALL(Mock, OnEndMap(false));

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnMapItem("a");
        builder->OnBeginMap();
        builder->OnMapItem("b");
            builder->OnBeginMap();
            builder->OnMapItem("c");
            builder->OnInt64Scalar(42, false);
            builder->OnEndMap(false);
        builder->OnEndMap(false);
    builder->OnEndMap(false);
    auto root = builder->EndTree();

    TTreeVisitor visitor(&Mock);
    visitor.Visit(root);
}

TEST_F(TTreeBuilderTest, MapWithAttributes)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());

    EXPECT_CALL(Mock, OnMapItem("mode"));
        EXPECT_CALL(Mock, OnInt64Scalar(755, false));

    EXPECT_CALL(Mock, OnMapItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello", false));

    EXPECT_CALL(Mock, OnEndMap(true));

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnAttributesItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());

        EXPECT_CALL(Mock, OnMapItem("read"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("*", false));
        EXPECT_CALL(Mock, OnEndList(false));

        EXPECT_CALL(Mock, OnMapItem("write"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("sandello", false));
        EXPECT_CALL(Mock, OnEndList(false));

        EXPECT_CALL(Mock, OnEndMap(false));

    EXPECT_CALL(Mock, OnAttributesItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables", false));

    EXPECT_CALL(Mock, OnEndAttributes());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();

    builder->OnMapItem("path");
        builder->OnStringScalar("/home/sandello", false);

    builder->OnMapItem("mode");
        builder->OnInt64Scalar(755, false);

    builder->OnEndMap(true);

    builder->OnBeginAttributes();
    builder->OnAttributesItem("acl");
        builder->OnBeginMap();

        builder->OnMapItem("read");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("*", false);
        builder->OnEndList(false);

        builder->OnMapItem("write");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("sandello", false);
        builder->OnEndList(false);

        builder->OnEndMap(false);

    builder->OnAttributesItem("lock_scope");
        builder->OnStringScalar("mytables", false);

    builder->OnEndAttributes();
    auto root = builder->EndTree();

    TTreeVisitor visitor(&Mock);
    visitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
