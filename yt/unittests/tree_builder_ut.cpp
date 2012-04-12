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
    EXPECT_CALL(Mock, OnMapItem("a"));
        EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnMapItem("b"));
            EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnMapItem("c"));
            EXPECT_CALL(Mock, OnIntegerScalar(42));
            EXPECT_CALL(Mock, OnEndMap());
        EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnMapItem("a");
        builder->OnBeginMap();
        builder->OnMapItem("b");
            builder->OnBeginMap();
            builder->OnMapItem("c");
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

    EXPECT_CALL(Mock, OnMapItem("mode"));
        EXPECT_CALL(Mock, OnIntegerScalar(755));

    EXPECT_CALL(Mock, OnMapItem("path"));
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello"));

    EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnBeginAttributes());
    EXPECT_CALL(Mock, OnAttributesItem("acl"));
        EXPECT_CALL(Mock, OnBeginMap());

        EXPECT_CALL(Mock, OnMapItem("read"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("*"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnMapItem("write"));
        EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("sandello"));
        EXPECT_CALL(Mock, OnEndList());

        EXPECT_CALL(Mock, OnEndMap());

    EXPECT_CALL(Mock, OnAttributesItem("lock_scope"));
        EXPECT_CALL(Mock, OnStringScalar("mytables"));

    EXPECT_CALL(Mock, OnEndAttributes());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();

    builder->OnMapItem("path");
        builder->OnStringScalar("/home/sandello");

    builder->OnMapItem("mode");
        builder->OnIntegerScalar(755);

    builder->OnEndMap();

    builder->OnBeginAttributes();
    builder->OnAttributesItem("acl");
        builder->OnBeginMap();

        builder->OnMapItem("read");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("*");
        builder->OnEndList();

        builder->OnMapItem("write");
        builder->OnBeginList();
        builder->OnListItem();
        builder->OnStringScalar("sandello");
        builder->OnEndList();

        builder->OnEndMap();

    builder->OnAttributesItem("lock_scope");
        builder->OnStringScalar("mytables");

    builder->OnEndAttributes();
    auto root = builder->EndTree();

    VisitTree(~root, &Mock);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
