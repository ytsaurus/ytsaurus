#include "common.h"

#include <yt/yt/orm/server/objects/attribute_matcher.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/orm/library/query/expression_evaluator.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

auto BuildMapMultipolicyIdWithMeta(TString strId, i64 i64Id, ui64 ui64Id, ui64 aui64Id)
{
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("meta")
                .BeginMap()
                    .Item("str_id").Value(strId)
                    .Item("i64_id").Value(i64Id)
                    .Item("ui64_id").Value(ui64Id)
                    .Item("another_ui64_id").Value(aui64Id)
                .EndMap()
        .EndMap()
        ->AsMap();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TMetaKeyTest, MetaKeyMultipolicy)
{
    auto typeHandler = GetBootstrap()->GetObjectManager()
        ->GetTypeHandlerOrCrash(TObjectTypeValues::MultipolicyId);
    auto object = BuildMapMultipolicyIdWithMeta("abcde", 252, 2435u, 12u);
    auto key = MatchKeyAttributes(typeHandler, object->FindChild("meta")).Key;

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();

    auto* testObject = transaction->GetObject(
        TObjectTypeValues::MultipolicyId,
        key);

    auto queryContext = MakeQueryContext(
        transaction->GetBootstrap(),
        TObjectTypeValues::MultipolicyId,
        transaction->GetSession(),
        /*indexSchema*/ nullptr,
        /*allowAnnotations*/ false);

    auto schema = testObject->GetTypeHandler()
        ->GetMetaAttributeSchema()
        ->FindChild("key")
        ->AsScalar();
    ASSERT_TRUE(schema->HasExpressionBuilder());

    auto buildedPtr = schema->RunExpressionBuilder(queryContext.get(), "", EAttributeExpressionContext::Fetch);
    auto queryExpression = NQueryClient::NAst::FormatExpression(NQueryClient::NAst::TExpressionList{std::move(buildedPtr)});
    auto valueKey = RunConsumedValueGetter(
        schema,
        transaction.Get(),
        testObject,
        ""
    )->AsString()->GetValue();

    NQueryClient::TSchemaColumns columns = {
        {
            "meta.str_id", NQueryClient::EValueType::String
        },
        {
            "meta.i64_id", NQueryClient::EValueType::Int64
        },
        {
            "meta.ui64_id", NQueryClient::EValueType::Uint64
        },
        {
            "meta.another_ui64_id", NQueryClient::EValueType::Uint64
        }};

    auto evaluator = NQuery::CreateExpressionEvaluator(queryExpression, std::move(columns));
    auto builderValue = evaluator->Evaluate({
        BuildYsonStringFluently().Value("abcde"),
        BuildYsonStringFluently().Value(252),
        BuildYsonStringFluently().Value(2435u),
        BuildYsonStringFluently().Value(12u)
    }).ValueOrThrow().AsString();

    ASSERT_EQ(valueKey, builderValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
