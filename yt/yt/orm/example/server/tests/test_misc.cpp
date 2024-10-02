#include "common.h"

#include <yt/yt/orm/example/server/library/autogen/objects.h>

#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TMiscTest, TestAttributeGetter)
{
    auto transaction = WaitFor(GetBootstrap()
        ->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();

    auto* publisher = transaction
        ->CreateObjectInternal(TObjectTypeValues::Publisher, TObjectKey(42))
        ->As<TPublisher>();

    auto* annotationsSchema = ResolveAttribute(publisher->GetTypeHandler(), "/annotations", {})
        .Attribute->AsScalar();
    auto* annotations = annotationsSchema->GetAttribute<TAnnotationsAttribute>(publisher);

    EXPECT_EQ(annotations, &publisher->Annotations());
}

TEST(TMiscTest, TestForbidNonEmptyRemoval)
{
    auto parentKey = CreateObject(
        TObjectTypeValues::Publisher,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("spec")
                    .BeginMap()
                        .Item("name").Value("ABC")
                    .EndMap()
            .EndMap()
            ->AsMap());
    auto alternativePublisherKey = CreateObject(
        TObjectTypeValues::Publisher,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("spec")
                    .BeginMap()
                        .Item("name").Value("BCD")
                    .EndMap()
            .EndMap()
            ->AsMap());

    auto book = CreateObject(
        TObjectTypeValues::Book,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("isbn").Value("978-1449355739")
                        .Item("parent_key").Value(parentKey.ToString())
                    .EndMap()
                .Item("spec")
                    .BeginMap()
                        .Item("alternative_publisher_ids")
                            .BeginList()
                                .Item().Value(std::get<i64>(alternativePublisherKey[0]))
                            .EndList()
                    .EndMap()
            .EndMap()
            ->AsMap());

    auto transaction = WaitFor(GetBootstrap()
        ->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();

    transaction->AllowRemovalWithNonEmptyReferences(true);
    EXPECT_NO_THROW(transaction->AllowRemovalWithNonEmptyReferences(true));
    EXPECT_THROW(transaction->AllowRemovalWithNonEmptyReferences(false), TErrorException);

    auto* publisher = transaction->GetObject(TObjectTypeValues::Publisher, alternativePublisherKey);
    transaction->RemoveObject(publisher);
    WaitFor(transaction->Commit())
        .ThrowOnError();
}

TEST(TMiscTest, TestFinalizingObjectUpdateForbidden)
{
    auto publisherKey = CreateObject(
        TObjectTypeValues::Publisher,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("finalizers")
                            .BeginMap()
                                .Item("fin").Value(NDataModel::TFinalizer())
                            .EndMap()
                    .EndMap()
            .EndMap()
            ->AsMap());

    {
        auto transaction = WaitFor(GetBootstrap()
            ->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        auto* publisher = transaction->GetTypedObject<TPublisher>(publisherKey);
        auto otherPublisherKey = transaction->CreateObjectInternal(TObjectTypeValues::Publisher, TObjectKey(42))
            ->GetKey();
        transaction->RemoveObject(publisher);
        auto* illustrator = transaction->CreateObjectInternal(
            TObjectTypeValues::Illustrator, TObjectKey(42), otherPublisherKey)
            ->As<TIllustrator>();

        EXPECT_THROW_WITH_ERROR_CODE(
            publisher->Status().Etc().MutableLoad(),
            NYT::NOrm::NClient::EErrorCode::InvalidObjectState);

        EXPECT_THROW_WITH_ERROR_CODE(
            publisher->Status().Illustrators().Add(illustrator),
            NYT::NOrm::NClient::EErrorCode::InvalidObjectState);

        transaction->CreateObjectInternal(TObjectTypeValues::Book, TObjectKey(42, 43), publisherKey);
        EXPECT_THROW_WITH_ERROR_CODE(
            transaction->Commit().Get().ThrowOnError(),
            NYT::NOrm::NClient::EErrorCode::InvalidObjectState);
    }

    {
        auto transaction = WaitFor(GetBootstrap()
            ->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        auto* publisher = transaction->GetTypedObject<TPublisher>(publisherKey);
        transaction->RemoveObject(publisher);
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    auto bookKey = CreateBook(GetEphemeralNodeFactory()->CreateMap());
    {
        auto transaction = WaitFor(GetBootstrap()
            ->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();

        auto* book = transaction->GetTypedObject<TBook>(bookKey);
        book->AddFinalizer("fin", "root");
        transaction->RemoveObject(book);
        auto* hitchhiker = transaction->CreateObjectInternal(TObjectTypeValues::Hitchhiker, TObjectKey(42))
            ->As<THitchhiker>();

        EXPECT_THROW_WITH_ERROR_CODE(
            book->Status().Hitchhikers().Add(hitchhiker),
            NYT::NOrm::NClient::EErrorCode::InvalidObjectState);
        EXPECT_THROW_WITH_ERROR_CODE(
            hitchhiker->Spec().Books().Add(book),
            NYT::NOrm::NClient::EErrorCode::InvalidObjectState);
    }
}

} // namespace NYT::NOrm::NExample::NServer::NTests
