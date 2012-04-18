#include "stdafx.h"

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/tree_visitor.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilderTest: public ::testing::Test
{
public:
    StrictMock<TMockYsonConsumer> Mock;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTreeBuilderTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnEndMap();
    auto root = builder->EndTree();

    VisitTree(~root, &Mock);
}

TEST_F(TTreeBuilderTest, NestedMaps)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("b"));
            EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("c"));
            EXPECT_CALL(Mock, OnIntegerScalar(42));
            EXPECT_CALL(Mock, OnEndMap());
        EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnKeyedItem("a");
        builder->OnBeginMap();
        builder->OnKeyedItem("b");
            builder->OnBeginMap();
            builder->OnKeyedItem("c");
            builder->OnIntegerScalar(42);
            builder->OnEndMap();
        builder->OnEndMap();
    builder->OnEndMap();
    auto root = builder->EndTree();

    VisitTree(~root, &Mock);
}

TEST_F(TTreeBuilderTest, MapWithAttributes)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());

    EXPECT_CALL(Mock, OnKeyedItem("mode"));
        EXPECT_CALL(Mock, OnIntegerScalar(755));

    EXPECT_CALL(Mock, OnKeyedItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello"));

    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnKeyedItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());

        EXPECT_CALL(Mock, OnKeyedItem("read"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("*"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnKeyedItem("write"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("sandello"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnKeyedItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables"));

    EXPECT_CALL(Mock, OnEndAttributes());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();

    builder->OnKeyedItem("path");
        builder->OnStringScalar("/home/sandello");

    builder->OnKeyedItem("mode");
        builder->OnIntegerScalar(755);

    builder->OnEndMap();

    builder->OnBeginAttributes();
    builder->OnKeyedItem("acl");
        builder->OnBeginMap();

        builder->OnKeyedItem("read");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("*");
        builder->OnEndList();

        builder->OnKeyedItem("write");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("sandello");
        builder->OnEndList();

        builder->OnEndMap();

    builder->OnKeyedItem("lock_scope");
        builder->OnStringScalar("mytables");

    builder->OnEndAttributes();
    auto root = builder->EndTree();

    VisitTree(~root, &Mock);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
