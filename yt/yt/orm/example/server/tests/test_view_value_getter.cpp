#include "common.h"

#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/object.h>
#include <yt/yt/orm/server/objects/object_reflection.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <array>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TViewValueGetterTestSuite
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TViewValueGetterTestSuite, TestManyToOneViewValueGetter)
{
    auto editorKey = CreateEditor();
    auto bookKey = CreateBook(
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("editor_id").Value(editorKey.ToString())
            .EndMap()
            ->AsMap());
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();

        auto* book = transaction->GetObject(TObjectTypeValues::Book, bookKey);
        auto* typeHandler = book->GetTypeHandler();
        auto resolveResult = NYT::NOrm::NServer::NObjects::ResolveAttribute(
            typeHandler,
            "/spec/editor/meta/id",
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto* schema = resolveResult.Attribute->AsScalar();
        auto editorId = RunConsumedValueGetter(schema, transaction.Get(), book, resolveResult.SuffixPath)->AsString()->GetValue();
        EXPECT_EQ(editorId, editorKey.ToString());
    }
}

TEST_F(TViewValueGetterTestSuite, TestManyToOneViewValueGetterNotExisting)
{
    auto bookKey = CreateBook(GetEphemeralNodeFactory()->CreateMap());
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();

        auto* book = transaction->GetObject(TObjectTypeValues::Book, bookKey);
        auto* typeHandler = book->GetTypeHandler();
        auto resolveResult = NYT::NOrm::NServer::NObjects::ResolveAttribute(
            typeHandler,
            "/spec/editor/meta/id",
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto* schema = resolveResult.Attribute->AsScalar();
        auto node = RunConsumedValueGetter(schema, transaction.Get(), book, resolveResult.SuffixPath);
        EXPECT_EQ(node->GetType(), NYT::NYTree::ENodeType::Entity);
    }
}

TEST_F(TViewValueGetterTestSuite, TestManyToManyViewValueGetter)
{
    std::array<TObjectKey, 2> authorKeys = {CreateAuthor(), CreateAuthor()};
    auto bookKey = CreateBook(
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("author_ids")
                    .BeginList()
                        .DoFor(authorKeys, [&] (TFluentList fluent, const auto& key) {
                            fluent.Item().Value(key.ToString());
                    })
                    .EndList()
            .EndMap()
            ->AsMap());
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();

        auto* book = transaction->GetObject(TObjectTypeValues::Book, bookKey);
        auto* typeHandler = book->GetTypeHandler();
        auto resolveResult = NYT::NOrm::NServer::NObjects::ResolveAttribute(
            typeHandler,
            "/spec/authors/*/meta/id",
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto* schema = resolveResult.Attribute->AsScalar();
        auto node = RunConsumedValueGetter(schema, transaction.Get(), book, resolveResult.SuffixPath);
        EXPECT_EQ(node->GetType(), NYT::NYTree::ENodeType::List);
        EXPECT_EQ(authorKeys.size(), (unsigned long) node->AsList()->GetChildCount());
        for (size_t i = 0; i < authorKeys.size(); ++i) {
            EXPECT_EQ(authorKeys[i].ToString(), node->AsList()->FindChild(i)->AsString()->GetValue());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
