#include "common.h"

#include <yt/yt/orm/server/objects/attribute_matcher.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

auto BuildManualIdWithMeta(i64 i64Id, ui64 ui64Id)
{
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("meta")
                .BeginMap()
                    .Item("i64_id").Value(i64Id)
                    .Item("ui64_id").Value(ui64Id)
                .EndMap()
        .EndMap()
        ->AsMap();
}

auto BuildManualIdWithDeficientMeta(i64 i64Id)
{
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("meta")
                .BeginMap()
                    .Item("i64_id").Value(i64Id)
                .EndMap()
        .EndMap()
        ->AsMap();
}

// 'abcde' as str_id has got from KeyEvaluator.
constexpr char ExpectedObjectKey[] = "abcde;252;25";

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeKeyTest, ShouldNotMatchKeyAttributesWithCustomEvaluator)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::ManualId);
    EXPECT_ANY_THROW(MatchKeyAttributes(typeHandler, BuildManualIdWithDeficientMeta(252)->FindChild("meta")));
}

TEST(TCompositeKeyTest, ShouldMatchKeyAttributesWithCustomEvaluator)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::ManualId);
    auto key = MatchKeyAttributes(typeHandler, BuildManualIdWithMeta(252, 25u)->FindChild("meta")).Key;
    ASSERT_EQ(key.ToString(), ExpectedObjectKey);
}

TEST(TCompositeKeyTest, ShouldNotCreate)
{
    EXPECT_ANY_THROW(CreateObject(TObjectTypeValues::ManualId, BuildManualIdWithDeficientMeta(252)));
}

TEST(TCompositeKeyTest, ShouldCreate)
{
    const auto manualIdKey = CreateObject(TObjectTypeValues::ManualId, BuildManualIdWithMeta(252, 25u));
    ASSERT_EQ(manualIdKey.ToString(), ExpectedObjectKey);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
