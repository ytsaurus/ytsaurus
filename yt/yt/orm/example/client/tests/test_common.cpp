#include "common.h"

#include <yt/yt/orm/example/client/proto/data_model/autogen/schema.pb.h>
#include <yt/yt/orm/example/client/proto/data_model/author.pb.h>
#include <yt/yt/orm/example/client/proto/data_model/book.pb.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>
#include <library/cpp/testing/gtest_protobuf/matcher.h>

#include <util/generic/guid.h>
#include <util/random/random.h>

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientFetchRootObjectTestSuite
    : public TNativeClientTestSuite
{
public:
    void SetUp() override
    {
        TNativeClientTestSuite::SetUp();

        PublisherId = RandomNumber<ui64>(1000000000);

        CreatePublisher(Client_, PublisherId);

        BookId1 = RandomNumber<ui64>(1000000000);
        BookId2 = RandomNumber<ui64>(1000000000);

        BookIdentity1 = CreateBook(Client_, PublisherId, BookId1, /*released*/ false);
        BookIdentity2 = CreateBook(Client_, PublisherId, BookId2, /*released*/ true);
    }

    static TObjectIdentity CreateAuthor(const IClientPtr& client, const TObjectId& authorId)
    {
        return WaitFor(client->CreateObject(
            TObjectTypeValues::Author,
            BuildAuthorPayload(authorId),
            TCreateObjectOptions{
                .Format = EPayloadFormat::Protobuf,
            }))
            .ValueOrThrow()
            .Meta;
    }

    TGetObjectResult GetBook(
        const TObjectIdentity& bookIdentity,
        const TAttributeSelector& selector,
        bool fetchRootObject,
        bool fetchTimestamps = false,
        bool ignoreNonexistent = false,
        EPayloadFormat format = EPayloadFormat::Protobuf)
    {
        return WaitFor(
            Client_->GetObject(
                bookIdentity,
                TObjectTypeValues::Book,
                selector,
                TGetObjectOptions{
                    .Format = format,
                    .IgnoreNonexistent = ignoreNonexistent,
                    .FetchTimestamps = fetchTimestamps,
                    .FetchRootObject = fetchRootObject,
                    .FetchPerformanceStatistics = true,
                }))
                .ValueOrThrow();
    }

    NProto::NDataModel::TBook GetBookProto(
        const TObjectIdentity& bookIdentity,
        const TAttributeSelector& selector,
        bool fetchTimestamps = false,
        bool ignoreNonexistent = false,
        EPayloadFormat format = EPayloadFormat::Protobuf)
    {
        auto getResult = GetBook(
            bookIdentity,
            selector,
            /*fetchRootObject*/ true,
            fetchTimestamps,
            ignoreNonexistent,
            format);
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResult.Result);
        return book;
    }

protected:
    i64 PublisherId;
    i64 BookId1;
    i64 BookId2;

    TObjectIdentity BookIdentity1;
    TObjectIdentity BookIdentity2;

    void DeserializeBookFromAttributeList(NProto::NDataModel::TBook* book, const TAttributeList& attributes)
    {
        ASSERT_EQ(3ul, attributes.ValuePayloads.size());

        EXPECT_TRUE(book->mutable_meta()->ParseFromString(std::get<TProtobufPayload>(attributes.ValuePayloads[0]).Protobuf));
        EXPECT_TRUE(book->mutable_spec()->ParseFromString(std::get<TProtobufPayload>(attributes.ValuePayloads[1]).Protobuf));
        ASSERT_TRUE(book->mutable_status()->ParseFromString(std::get<TProtobufPayload>(attributes.ValuePayloads[2]).Protobuf));
    }

    void DeserializeBookFromAttributeListTopLevel(NProto::NDataModel::TBook* book, const TAttributeList& attributes)
    {
        ASSERT_EQ(1ul, attributes.ValuePayloads.size());
        ASSERT_TRUE(book->ParseFromString(std::get<TProtobufPayload>(attributes.ValuePayloads[0]).Protobuf));
    }

protected:
    TYsonPayload BuildBookIdPayload(const i64 id)
    {
        return BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("isbn").Value("ISBN")
                            .Item("id").Value(id)
                            .Item("id2").Value(BookId2Always_)
                            .Item("publisher_id").Value(PublisherId)
                        .EndMap()
                .EndMap();
        }));
    }

    static TYsonPayload BuildAuthorPayload(const TObjectId& authorId)
    {
        return BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("id").Value(authorId)
                        .EndMap()
                    .Item("spec")
                        .BeginMap()
                            .Item("name").Value("Mark Lutz")
                        .EndMap()
                .EndMap();
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNativeClientPartialGetObjectSuite
    : public TNativeClientFetchRootObjectTestSuite
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientPartialGetObjectSuite, TestRepeatedItemByIndexNotSupported)
{
    ASSERT_THROW(
        GetBook(
            BookIdentity1,
            {"/spec/genres/0"},
            /*fetchRootObject*/ true),
        NYT::TErrorException);
}

TEST_F(TNativeClientPartialGetObjectSuite, TestIntersectingSelectors)
{
    auto getResult = GetBook(
        BookIdentity1,
        {
            "/spec/design/color",
            "/spec/design/cover/hardcover",
            "/spec/design/cover",
            "/meta/ultimate_question_of_life"
        },
        /*fetchRootObject*/ true).Result;

    NProto::NDataModel::TBook expectedBook;
    expectedBook.mutable_spec()->mutable_design()->set_color("Black");
    expectedBook.mutable_spec()->mutable_design()->mutable_cover()->set_size(1024);
    expectedBook.mutable_spec()->mutable_design()->mutable_cover()->set_dpi(-300);
    expectedBook.mutable_spec()->mutable_design()->mutable_cover()->set_hardcover(true);
    expectedBook.mutable_meta()->set_ultimate_question_of_life(42);

    NProto::NDataModel::TBook book;
    DeserializeBookFromAttributeListTopLevel(&book, getResult);

    EXPECT_THAT(book, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientPartialGetObjectSuite, TestCompatibility)
{
    NProto::NDataModel::TBook bookFromParts;
    DeserializeBookFromAttributeList(
        &bookFromParts,
        GetBook(
            BookIdentity1,
            {"/meta", "/spec", "/status"},
            /*fetchRootObject*/ false)
            .Result);

    NProto::NDataModel::TBook bookTopLevel;
    DeserializeBookFromAttributeListTopLevel(
        &bookTopLevel,
        GetBook(
            BookIdentity1,
            {"/meta", "/spec", "/status"},
            /*fetchRootObject*/ true)
            .Result);

    EXPECT_EQ(BookId1, bookFromParts.meta().id());
    EXPECT_EQ(ToString(BookId1), bookFromParts.spec().name());
    EXPECT_EQ(false, bookFromParts.status().released());
    bookFromParts.mutable_meta()->clear_finalization_start_time();

    EXPECT_THAT(bookTopLevel, NGTest::EqualsProto(std::ref(bookFromParts)));
}

TEST_F(TNativeClientPartialGetObjectSuite, TestParts)
{
    TAttributeSelector selector{
        "/spec/year",
        "/spec/genres",
        "/spec/design/cover/size",
        "/spec/design/cover/dpi"};
    auto getResult = GetBook(
        BookIdentity1,
        selector,
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ true)
        .Result;

    EXPECT_EQ(selector.size(), getResult.Timestamps.size());

    NProto::NDataModel::TBook book;
    DeserializeBookFromAttributeListTopLevel(&book, getResult);

    NProto::NDataModel::TBook expectedBook;
    expectedBook.mutable_spec()->set_year(2021);
    expectedBook.mutable_spec()->add_genres("Genre 1");
    expectedBook.mutable_spec()->add_genres("Genre 2");
    expectedBook.mutable_spec()->mutable_design()->mutable_cover()->set_size(1024);
    expectedBook.mutable_spec()->mutable_design()->mutable_cover()->set_dpi(-300);

    EXPECT_THAT(book, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientPartialGetObjectSuite, TestNonexistentWithFetchTimestamps)
{
    TAttributeSelector selector{"/spec/year", "/spec/genres"};
    i64 bookId = RandomNumber<ui64>(1000000000);
    auto identity = BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("id").Value(bookId)
                .Item("id2").Value(BookId2Always_)
            .EndMap();
    }));

    auto getResult = GetBook(
        identity,
        selector,
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ true,
        /*ignoreNonexistent*/ true)
        .Result;

    EXPECT_EQ(0ul, getResult.Timestamps.size());
    EXPECT_EQ(0ul, getResult.ValuePayloads.size());
}

TEST_F(TNativeClientPartialGetObjectSuite, TestRootSelector)
{
    auto getBook = [&] (bool fetchRootObject) {
        auto getResult = GetBook(
            BookIdentity1,
            TAttributeSelector{NYPath::TYPath()},
            fetchRootObject,
            /*fetchTimestamps*/ false,
            /*ignoreNonexistent*/ true)
            .Result;

        EXPECT_TRUE(getResult.Timestamps.empty());

        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResult);
        return book;
    };

    auto expectedBook = getBook(true);
    auto actualBook = getBook(false);

    EXPECT_THAT(actualBook, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientPartialGetObjectSuite, TestEmptySelector)
{
    auto getResult = GetBook(
        BookIdentity1,
        TAttributeSelector{},
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ false,
        /*ignoreNonexistent*/ true)
        .Result;

    EXPECT_TRUE(getResult.Timestamps.empty());

    NProto::NDataModel::TBook book;
    DeserializeBookFromAttributeListTopLevel(&book, getResult);

    EXPECT_THAT(NProto::NDataModel::TBook(), NGTest::EqualsProto(std::ref(book)));
}

////////////////////////////////////////////////////////////////////////////////

class TNativeClientPartialGetObjectsSuite
    : public TNativeClientFetchRootObjectTestSuite
{
public:
    TGetObjectsResult GetBooks(
        std::vector<TObjectIdentity> bookIdentities,
        const TAttributeSelector& selector,
        bool fetchRootObject,
        bool fetchTimestamps = false,
        bool skipNonexistent = false,
        EPayloadFormat format = EPayloadFormat::Protobuf)
    {
        return WaitFor(Client_->GetObjects(
            std::move(bookIdentities),
            TObjectTypeValues::Book,
            selector,
            TGetObjectOptions{
                .Format = format,
                .SkipNonexistent = skipNonexistent,
                .FetchTimestamps = fetchTimestamps,
                .FetchRootObject = fetchRootObject,
            }))
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientPartialGetObjectsSuite, TestParts)
{
    TAttributeSelector selector{"/spec/year", "/spec/genres", "/meta/id", "/meta/isbn"};
    auto getResults = GetBooks(
        {BookIdentity1, BookIdentity2},
        selector,
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ true)
        .Subresults;

    ASSERT_EQ(2ul, getResults.size());

    EXPECT_EQ(selector.size(), getResults[0].Timestamps.size());
    EXPECT_EQ(selector.size(), getResults[1].Timestamps.size());

    NProto::NDataModel::TBook expectedBook;
    expectedBook.mutable_spec()->set_year(2021);
    expectedBook.mutable_spec()->add_genres("Genre 1");
    expectedBook.mutable_spec()->add_genres("Genre 2");
    expectedBook.mutable_meta()->set_isbn("ISBN");
    expectedBook.mutable_meta()->set_id(BookId1);

    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[0]);
        EXPECT_THAT(book, NGTest::EqualsProto(std::ref(expectedBook)));
    }
    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[1]);

        expectedBook.mutable_meta()->set_id(BookId2);
        EXPECT_THAT(book, NGTest::EqualsProto(std::ref(expectedBook)));
    }
}

TEST_F(TNativeClientPartialGetObjectsSuite, TestSkipNonexistent)
{
    TAttributeSelector selector{"/meta/id"};
    NProto::NDataModel::TBookMeta bookMeta;
    bookMeta.set_id(RandomNumber<ui64>(1000000000));
    bookMeta.set_id2(BookId2Always_);
    TObjectIdentity nonexistBookIdentity{TProtobufPayload{.Protobuf = bookMeta.SerializeAsString()}};
    auto getResults = GetBooks(
        {BookIdentity1, nonexistBookIdentity, BookIdentity2},
        selector,
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ false,
        /*skipNonexistent*/ true)
        .Subresults;

    ASSERT_EQ(2ul, getResults.size());
    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[0]);
        EXPECT_EQ(BookId1, book.meta().id());
    }
    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[1]);
        EXPECT_EQ(BookId2, book.meta().id());
    }
}

TEST_F(TNativeClientPartialGetObjectsSuite, TestSkipNonexistentAndMatchUuid)
{
    TAttributeSelector selector{"/meta/id"};
    NProto::NDataModel::TBookMeta bookMeta;
    bookMeta.set_id(RandomNumber<ui64>(1000000000));
    bookMeta.set_id2(BookId2Always_);
    TObjectIdentity nonexistBookIdentity{TProtobufPayload{.Protobuf = bookMeta.SerializeAsString()}};
    TObjectIdentity bookIdentity2WithUid{TProtobufPayload{
        .Protobuf = GetBookProto(BookIdentity2, {"/meta/id", "/meta/id2", "/meta/uuid"}).meta().SerializeAsString()
    }};

    auto getResults = GetBooks(
        {BookIdentity1, nonexistBookIdentity, bookIdentity2WithUid},
        selector,
        /*fetchRootObject*/ true,
        /*fetchTimestamps*/ false,
        /*skipNonexistent*/ true)
        .Subresults;

    ASSERT_EQ(2ul, getResults.size());
    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[0]);
        EXPECT_EQ(BookId1, book.meta().id());
    }
    {
        NProto::NDataModel::TBook book;
        DeserializeBookFromAttributeListTopLevel(&book, getResults[1]);
        EXPECT_EQ(BookId2, book.meta().id());
    }
}

TEST_F(TNativeClientPartialGetObjectsSuite, TestSkipNonexistentAndIgnoreNonExistentAreMutualExclusive)
{
    ASSERT_THROW(YT_UNUSED_FUTURE(Client_->GetObjects(
        {BookIdentity1},
        TObjectTypeValues::Book,
        {},
        TGetObjectOptions{
            .Format = EPayloadFormat::Protobuf,
            .IgnoreNonexistent = true,
            .SkipNonexistent = true,
        })),
        NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

class TNativeClientPartialSelectObjectsSuite
    : public TNativeClientFetchRootObjectTestSuite
{
public:
    TSelectObjectsResult SelectBooks(
        const TAttributeSelector& selector,
        TString filter,
        bool fetchRootObject,
        bool fetchTimestamps = false,
        EPayloadFormat format = EPayloadFormat::Protobuf,
        bool allowFullScan = true)
    {
        return WaitFor(
            Client_->SelectObjects(
                TObjectTypeValues::Book,
                selector,
                TSelectObjectsOptions{
                    .Format = format,
                    .Filter = std::move(filter),
                    .FetchTimestamps = fetchTimestamps,
                    .FetchRootObject = fetchRootObject,
                    .AllowFullScan = allowFullScan,
                }))
                .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientPartialSelectObjectsSuite, TestFullScanFailure)
{
    TAttributeSelector selector{"/spec/year", "/spec/genres", "/meta/id", "/meta/isbn", "/status"};
    auto selectBooks = [&] (bool allowFullScan = true) {
        return SelectBooks(
            selector,
            /*filter*/ TString{"[/spec/name] = \""} + ToString(BookId2) + '\"',
            /*fetchRootObject*/ true,
            /*fetchTimestamps*/ true,
            EPayloadFormat::Protobuf,
            /*allowFullScan*/ allowFullScan)
            .Results;
    };

    EXPECT_THROW_WITH_SUBSTRING(selectBooks(/*allowFullScan*/ false), "the query is inefficient");
}

TEST_F(TNativeClientPartialSelectObjectsSuite, TestParts)
{
    TAttributeSelector selector{"/spec/year", "/spec/genres", "/meta/id", "/meta/isbn", "/status"};
    auto selectBooks = [&] (bool allowFullScan = true) {
        return SelectBooks(
            selector,
            /*filter*/ TString{"[/spec/name] = \""} + ToString(BookId2) + '\"',
            /*fetchRootObject*/ true,
            /*fetchTimestamps*/ true,
            EPayloadFormat::Protobuf,
            /*allowFullScan*/ allowFullScan)
            .Results;
    };

    auto selectResults = selectBooks();

    ASSERT_EQ(1ul, selectResults.size());

    EXPECT_EQ(selector.size(), selectResults[0].Timestamps.size());

    NProto::NDataModel::TBook expectedBook;
    expectedBook.mutable_spec()->set_year(2021);
    expectedBook.mutable_spec()->add_genres("Genre 1");
    expectedBook.mutable_spec()->add_genres("Genre 2");
    expectedBook.mutable_meta()->set_isbn("ISBN");
    expectedBook.mutable_meta()->set_id(BookId2);
    expectedBook.mutable_status()->set_released(true);

    NProto::NDataModel::TBook book;
    DeserializeBookFromAttributeListTopLevel(&book, selectResults[0]);
    EXPECT_THAT(book, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientPartialSelectObjectsSuite, TestOrderBy)
{
    TAttributeSelector selector{"/status/released"};
    auto selectBooks = [&] (const TObjectOrderBy& orderBy)
    {
        return WaitFor(Client_->SelectObjects(
            TObjectTypeValues::Book,
            selector,
            TSelectObjectsOptions{
                .Format = EPayloadFormat::Protobuf,
                .Limit = 1,
                .OrderBy = orderBy,
                .FetchRootObject = true,
                .AllowFullScan = true,
            }))
            .ValueOrThrow().Results;
    };

    NProto::NDataModel::TBook book;

    auto results = selectBooks({{{.Expression = "[/status/released]", .Descending = false}}});
    EXPECT_EQ(std::ssize(results), 1);
    DeserializeBookFromAttributeListTopLevel(&book, results[0]);
    EXPECT_FALSE(book.status().released());

    results = selectBooks({{{.Expression = "[/status/released]", .Descending = true}}});
    EXPECT_EQ(std::ssize(results), 1);
    DeserializeBookFromAttributeListTopLevel(&book, results[0]);
    EXPECT_TRUE(book.status().released());
}

////////////////////////////////////////////////////////////////////////////////

class TNativeClientUpdateTestSuite
    : public TNativeClientPartialGetObjectSuite
{
public:
    void UpdateBook(const TObjectIdentity& identity, const std::vector<TUpdate>& patch)
    {
        WaitFor(Client_->UpdateObject(
            identity,
            TObjectTypeValues::Book,
            patch))
            .ThrowOnError();
    }

    void RunRemoveUpdate(const TObjectIdentity& identity, NYPath::TYPath removePath, bool force = false)
    {
        UpdateBook(
            identity,
            {
                TRemoveUpdate{
                    .Path = std::move(removePath),
                    .Force = force,
                }
            });
    }

    void UpdateAuthor(const TObjectIdentity& identity)
    {
        auto patch = std::vector<TUpdate>{TSetUpdate{
            "/spec/name",
            BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer).Value("Mann");
            }))}};
        WaitFor(Client_->UpdateObject(
            identity,
            TObjectTypeValues::Author,
            patch))
            .ThrowOnError();
    }

    NProto::NDataModel::TAuthor GetAuthorProto(
        const TObjectIdentity& identity,
        TTransactionId transactionId = TTransactionId())
    {
        auto result = WaitFor(Client_->GetObject(
            identity,
            TObjectTypeValues::Author,
            {"/spec/name"},
            TGetObjectOptions{
                .Format = EPayloadFormat::Protobuf,
                .FetchRootObject = true,
                .TimestampByTransactionId = transactionId,
            }))
            .ValueOrThrow();

        NProto::NDataModel::TAuthor author;
        EXPECT_TRUE(author.ParseFromString(std::get<TProtobufPayload>(result.Result.ValuePayloads[0]).Protobuf));
        return author;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientUpdateTestSuite, TestIgnoreNonexistent)
{
    auto identity = CreateAuthor(
        Client_,
        /*authorId*/ CreateGuidAsString());

    WaitFor(Client_->RemoveObject(identity, TObjectTypeValues::Author))
        .ValueOrThrow();
    WaitFor(Client_->UpdateObject(
        identity,
        TObjectTypeValues::Author,
        std::vector<TUpdate>{
            TSetUpdate{
                "/spec/name",
                BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                    NYTree::BuildYsonFluently(consumer).Value("Mann");
                }))
            }
        },
        {},
        TUpdateObjectOptions{
            .IgnoreNonexistent = true,
        }))
        .ValueOrThrow();
}

TEST_F(TNativeClientUpdateTestSuite, TestRemoveExistingAttribute)
{
    auto expectedBook = GetBookProto(BookIdentity1, {"/spec"});
    EXPECT_EQ(expectedBook.spec().page_count(), 64ull);

    RunRemoveUpdate(BookIdentity1, "/spec/page_count");
    expectedBook.mutable_spec()->clear_page_count();

    auto actualBook = GetBookProto(BookIdentity1, {"/spec"});

    // Do not take into account the revision value.
    expectedBook.mutable_spec()->set_api_revision(0);
    actualBook.mutable_spec()->set_api_revision(0);
    EXPECT_THAT(actualBook, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientUpdateTestSuite, TestRemoveNotExistingAttribute)
{
    auto expectedBook = GetBookProto(BookIdentity1, {"/spec"});

    EXPECT_THROW(
        RunRemoveUpdate(BookIdentity1, "/spec/design/cover/image"),
        NYT::TErrorException);

    auto actualBook = GetBookProto(BookIdentity1, {"/spec"});
    EXPECT_THAT(actualBook, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientUpdateTestSuite, TestRemoveNotExistingAttributeForce)
{
    auto expectedBook = GetBookProto(BookIdentity1, {"/spec"});

    RunRemoveUpdate(BookIdentity1, "/spec/design/cover/image", /*force*/ true);

    auto actualBook = GetBookProto(BookIdentity1, {"/spec"});
    EXPECT_THAT(actualBook, NGTest::EqualsProto(std::ref(expectedBook)));
}

TEST_F(TNativeClientUpdateTestSuite, TestBook)
{
    auto identity = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1000000000),
        /*released*/ false);

    auto patch = std::vector<TUpdate>{TSetUpdate{
        "/spec/year",
        BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
            NYTree::BuildYsonFluently(consumer).Value(2015);
        }))}};
    UpdateBook(identity, patch);

    auto actualBook = GetBookProto(identity, {"/spec/year"});
    EXPECT_EQ(2015, actualBook.spec().year());
}

TEST_F(TNativeClientUpdateTestSuite, TestAuthor)
{
    auto identity = CreateAuthor(
        Client_,
        /*authorId*/ CreateGuidAsString());
    UpdateAuthor(identity);
    auto actual = GetAuthorProto(identity);
    ASSERT_EQ("Mann", actual.spec().name());
}

TEST_F(TNativeClientUpdateTestSuite, TestSetRootUpdate)
{
    auto identity = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1000000000),
        /*released*/ false);

    NProto::NDataModel::TBook book;
    book.mutable_spec()->set_year(2016);
    book.mutable_spec()->set_year(2016);
    book.mutable_spec()->mutable_digital_data()->set_store_rating(10.0);
    auto patch = std::vector<TUpdate>{TSetRootUpdate{
        .Paths = {"/spec/year", "/spec/digital_data/store_rating"},
        .Payload = TProtobufPayload{.Protobuf = book.SerializeAsString()}
    }};

    UpdateBook(identity, patch);

    auto actualBook = GetBookProto(identity, {"/spec"});
    EXPECT_EQ(2016, actualBook.spec().year());
    EXPECT_EQ(10.0, actualBook.spec().digital_data().store_rating());
}

TEST_F(TNativeClientUpdateTestSuite, TestSetRootUpdateInvalidPath)
{
    auto identity = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1000000000),
        /*released*/ false);

    NProto::NDataModel::TBook book;
    book.mutable_spec()->set_year(2016);
    book.mutable_spec()->set_year(2016);
    book.mutable_spec()->mutable_digital_data()->set_store_rating(10.0);
    auto patchInvalid1 = std::vector<TUpdate>{TSetRootUpdate{
        .Paths = {"/spec/year/missing"},
        .Payload = TProtobufPayload{.Protobuf = book.SerializeAsString()}
    }};

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(UpdateBook(identity, patchInvalid1),
        NYT::TErrorException,
        "Node /spec/year cannot have children");

    auto patchInvalid2 = std::vector<TUpdate>{TSetRootUpdate{
        .Paths = {"/spec/unknown_field"},
        .Payload = TProtobufPayload{.Protobuf = book.SerializeAsString()}
    }};

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(UpdateBook(identity, patchInvalid2),
        NYT::TErrorException,
        "No such field /spec/unknown_field");
}

TEST_F(TNativeClientUpdateTestSuite, TestRepeatedFieldSetRootUpdate)
{
    auto identity = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1000000000),
        /*released*/ false);

    NProto::NDataModel::TBook book;
    book.mutable_spec()->mutable_design()->add_illustrations("first");
    book.mutable_spec()->mutable_design()->add_illustrations("second");
    book.mutable_spec()->mutable_chapter_descriptions()->Add()->set_name("I");
    book.mutable_spec()->mutable_chapter_descriptions()->Add()->set_name("II");
    book.mutable_spec()->mutable_digital_data()->add_available_formats()->set_size(42);
    book.mutable_spec()->mutable_digital_data()->mutable_available_formats(0)->set_format(
        NProto::NDataModel::TBookDescription_TDigitalData_TFileData_EFormat::
            TBookDescription_TDigitalData_TFileData_EFormat_F_FB2);
    book.mutable_spec()->mutable_digital_data()->add_available_formats()->set_size(43);

    auto updates = TSetRootUpdate{
        .Paths{
            "/spec/chapter_descriptions",
            "/spec/design/illustrations",
            "/spec/digital_data/available_formats",
        },
        .Payload = TProtobufPayload{.Protobuf = book.SerializeAsString()},
    };

    UpdateBook(identity, {updates});
    auto getPaths = updates.Paths;
    NProto::NDataModel::TBook updatedBook = GetBookProto(identity, getPaths);
    auto compare = [&] {
        EXPECT_EQ(2, updatedBook.spec().design().illustrations().size());
        EXPECT_EQ(book.spec().design().illustrations()[0], updatedBook.spec().design().illustrations()[0]);
        EXPECT_EQ(book.spec().design().illustrations()[1], updatedBook.spec().design().illustrations()[1]);

        EXPECT_EQ(2, updatedBook.spec().chapter_descriptions().size());
        EXPECT_EQ(book.spec().chapter_descriptions()[0].name(), updatedBook.spec().chapter_descriptions()[0].name());
        EXPECT_EQ(book.spec().chapter_descriptions()[1].name(), updatedBook.spec().chapter_descriptions()[1].name());

        EXPECT_EQ(2, updatedBook.spec().digital_data().available_formats().size());
        EXPECT_EQ(book.spec().digital_data().available_formats()[0].size(),
            updatedBook.spec().digital_data().available_formats()[0].size());
        EXPECT_EQ(book.spec().digital_data().available_formats()[1].format(),
            updatedBook.spec().digital_data().available_formats()[1].format());
    };

    compare();

    updates.Paths[0].append("/0");
    updates.Paths[1].append("/0");
    updates.Paths[2].append("/0");
    book.mutable_spec()->mutable_design()->set_illustrations(0, "third");
    book.mutable_spec()->mutable_chapter_descriptions(0)->set_name("III");
    updates.Payload = TProtobufPayload{.Protobuf = book.SerializeAsString()};
    UpdateBook(identity, {updates});
    updatedBook = GetBookProto(identity, getPaths);

    compare();
}

TEST_F(TNativeClientUpdateTestSuite, TestAppend)
{
    auto identity = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1000000000),
        /*released*/ false);

    NProto::NDataModel::TBook book;
    auto* description = book.mutable_spec()->add_chapter_descriptions();
    description->set_name("Last");
    auto* format = book.mutable_spec()->mutable_digital_data()->add_available_formats();
    format->set_size(42);
    UpdateBook(identity, {
        TSetUpdate{
            .Path = "/spec/chapter_descriptions/end",
            .Payload = TProtobufPayload{.Protobuf = description->SerializeAsString()},
        },
        TSetUpdate{
            .Path = "/spec/digital_data/available_formats/end",
            .Payload = TProtobufPayload{.Protobuf = format->SerializeAsString()},
        },
        TSetUpdate{
            .Path = "/spec/design/illustrations/end",
            .Payload = TYsonPayload{NYTree::BuildYsonStringFluently().Value("first_image")},
        },
    });
    auto updatedBook = GetBookProto(identity, {"/spec"});

    EXPECT_EQ(1, updatedBook.spec().chapter_descriptions().size());
    EXPECT_EQ("Last", updatedBook.spec().chapter_descriptions()[0].name());

    EXPECT_EQ(1, updatedBook.spec().digital_data().available_formats().size());
    EXPECT_EQ(42ull, updatedBook.spec().digital_data().available_formats()[0].size());

    EXPECT_EQ(1, updatedBook.spec().design().illustrations().size());
    EXPECT_EQ("first_image", updatedBook.spec().design().illustrations(0));
}

TEST_F(TNativeClientUpdateTestSuite, TestTimestampByTransactionId)
{
    // By default [/spec/name]="Mark Lutz".
    auto identity = CreateAuthor(
        Client_,
        /*authorId*/ CreateGuidAsString());
    auto transactionId = WaitFor(Client_->StartTransaction())
        .ValueOrThrow()
        .TransactionId;
    // Set [/spec/name]="Mann".
    UpdateAuthor(identity);

    auto actualNoTimestamp = GetAuthorProto(identity);
    ASSERT_EQ("Mann", actualNoTimestamp.spec().name());

    auto actualWithTimestamp = GetAuthorProto(identity, transactionId);
    ASSERT_EQ("Mark Lutz", actualWithTimestamp.spec().name());

    WaitFor(Client_->CommitTransaction(transactionId))
        .ThrowOnError();
}

TEST_F(TNativeClientUpdateTestSuite, TestBothTimestampAndTimestampByTransactionId)
{
    auto transactionId = WaitFor(Client_->StartTransaction())
        .ValueOrThrow()
        .TransactionId;
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->GetObject(
            BookIdentity1,
            TObjectTypeValues::Book,
            {"/spec/name"},
            TGetObjectOptions{
                .Timestamp = 1ul,
                .Format = EPayloadFormat::Protobuf,
                .FetchRootObject = true,
                .TimestampByTransactionId = transactionId,
            }))
            .ValueOrThrow(),
        "Only one of `timestamp`, `timestamp_by_transaction_id`, `transaction_id` options could be specified");
}

TEST_F(TNativeClientUpdateTestSuite, TestWatchLog)
{
    auto meta = WaitFor(Client_->CreateObject(
        TObjectTypeValues::Executor,
        {},
        TCreateObjectOptions{
            .Format = EPayloadFormat::Yson,
        }))
        .ValueOrThrow()
        .Meta;

    auto startTimestamp = WaitFor(Client_->GenerateTimestamp())
        .ValueOrThrow()
        .Timestamp;

    WaitFor(Client_->UpdateObject(
        TObjectIdentity{meta},
        TObjectTypeValues::Executor,
        std::vector<TUpdate>{
            TSetUpdate{
                "/labels/tribe",
                BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                    NYTree::BuildYsonFluently(consumer).Value("Khalai");
                }))
            },
        }))
        .ValueOrThrow();

    auto timestamp = WaitFor(Client_->GenerateTimestamp())
        .ValueOrThrow()
        .Timestamp;

    auto result = WaitFor(Client_->WatchObjects(
        TObjectTypeValues::Executor,
        TWatchObjectsOptions{
            .StartTimestamp = startTimestamp,
            .Timestamp = timestamp,
            .TimeLimit = TDuration::Seconds(10),
            .Selector = TAttributeSelector{
                "/labels/tribe",
            },
            .FetchChangedAttributes = true,
            .WatchLog = "selector_watch_log",
        }))
        .ValueOrThrow();

    EXPECT_EQ(result.ChangedAttributesIndex.size(), 1ull);
    EXPECT_EQ(result.Events.size(), 1ull);
    EXPECT_EQ(result.Events[0].ChangedAttributesSummary.size(), 1ull);
    EXPECT_EQ(result.Events[0].ChangedAttributesSummary[0], 't');
}

////////////////////////////////////////////////////////////////////////////////

class TNativeClientRemoveObjectSuite
    : public TNativeClientPartialGetObjectSuite
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientRemoveObjectSuite, RemoveBook)
{
    {
        auto error = WaitFor(Client_->RemoveObject(BookIdentity1, TObjectTypeValues::Book));
        EXPECT_TRUE(error.IsOK()) << ToString(error);
    }
    {
        auto error = WaitFor(Client_->RemoveObject(BookIdentity1, TObjectTypeValues::Book));
        EXPECT_FALSE(error.IsOK());
        EXPECT_TRUE(error.FindMatching(NOrm::NClient::EErrorCode::NoSuchObject)) << ToString(error);
    }
    {
        auto error = WaitFor(Client_->RemoveObject(
            BookIdentity1,
            TObjectTypeValues::Book,
            TRemoveObjectOptions{
                .IgnoreNonexistent = true,
            }));
        EXPECT_TRUE(error.IsOK()) << ToString(error);
    }
}

TEST_F(TNativeClientRemoveObjectSuite, RemoveBooks)
{
    std::vector<TRemoveObjectsSubrequest> subrequests{
        TRemoveObjectsSubrequest{
            .ObjectIdentity = BookIdentity1,
            .ObjectType = TObjectTypeValues::Book,
        },
    };

    {
        auto error = WaitFor(Client_->RemoveObjects(subrequests));
        EXPECT_TRUE(error.IsOK()) << ToString(error);
    }

    subrequests.push_back(TRemoveObjectsSubrequest{
        .ObjectIdentity = BookIdentity2,
        .ObjectType = TObjectTypeValues::Book,
    });
    {
        auto error = WaitFor(Client_->RemoveObjects(subrequests));
        EXPECT_FALSE(error.IsOK());
        EXPECT_TRUE(error.FindMatching(NOrm::NClient::EErrorCode::NoSuchObject)) << ToString(error);
    }
    EXPECT_NO_THROW(GetBook(BookIdentity2, {"/meta"}, true));
    {
        auto error = WaitFor(Client_->RemoveObjects(
            subrequests,
            TRemoveObjectOptions{
                .IgnoreNonexistent = true,
            }));
        EXPECT_TRUE(error.IsOK()) << ToString(error);
    }
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        GetBook(BookIdentity2, {"/meta"}, true),
        NYT::TErrorException,
        "does not exist");
}

////////////////////////////////////////////////////////////////////////////////

class TNativeClientCreateObjects
    : public TNativeClientFetchRootObjectTestSuite
{ };

TEST_F(TNativeClientCreateObjects, UpsertBasic)
{
    std::vector<TUpdate> updates = {
        TLockUpdate{
            .Path = "/status/released",
            .LockType = NTableClient::ELockType::SharedStrong,
        },
        TSetUpdate{
            .Path = "/spec/name",
            .Payload = BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer).Value("Mann");
            }))
        },
        TRemoveUpdate{
            .Path = "/spec/page_count",
        }
    };
    CreateBook(
        Client_,
        PublisherId,
        BookId1,
        /*released*/ false,
        TUpdateIfExisting{.Updates = updates});
    auto expectedBook = GetBookProto(BookIdentity1, {"/spec"});
    EXPECT_FALSE(expectedBook.spec().has_page_count());
    EXPECT_EQ(expectedBook.spec().name(), "Mann");

    std::get<TSetUpdate>(updates[1]).Payload = BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer).Value("Learning Python");
    }));
    std::get<TRemoveUpdate>(updates[2]).Path = "/spec/design";

    auto result = WaitFor(Client_->CreateObjects({
        TCreateObjectsSubrequest{
            .ObjectType = TObjectTypeValues::Book,
            .AttributesPayload = BuildBookIdPayload(BookId1),
            .UpdateIfExisting = TUpdateIfExisting{.Updates = updates}
        },
        TCreateObjectsSubrequest{
            .ObjectType = TObjectTypeValues::Book,
            .AttributesPayload = BuildBookIdPayload(BookId2),
            .UpdateIfExisting = TUpdateIfExisting{.Updates = updates}
        }},
        TCreateObjectOptions{
            .Format = EPayloadFormat::Protobuf,
            .FetchPerformanceStatistics = true,
        }))
        .ValueOrThrow();

    EXPECT_EQ(result.PerformanceStatistics.ReadPhaseCount, 4);

    for (auto& bookIdentity : {BookIdentity1, BookIdentity2}) {
        auto expectedBook = GetBookProto(bookIdentity, {"/spec"});
        EXPECT_FALSE(expectedBook.spec().has_design());
        EXPECT_EQ(expectedBook.spec().name(), "Learning Python");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNativeSelectObjectHistoryTestSuite
    : public TNativeClientUpdateTestSuite
{
public:
    using TTimestamp = NTransactionClient::TTimestamp;

    TSelectObjectHistoryResult SelectBookHistory(
        const TObjectIdentity& identity,
        TTimestamp startTimestamp,
        std::optional<TAttributeSelector> distinctBy = std::nullopt)
    {
        TSelectObjectHistoryOptions options;
        options.TimestampInterval.Begin = startTimestamp;
        options.Distinct = true;

        return WaitFor(Client_->SelectObjectHistory(
            identity,
            TObjectTypeValues::Book,
            /*selector*/ {
                "/spec"
            },
            distinctBy,
            options))
            .ValueOrThrow();
    }

    TTimestamp GenerateTimestamp()
    {
        return WaitFor(Client_->GenerateTimestamp())
            .ValueOrThrow().Timestamp;
    }

    template <typename TValue>
    TSetUpdate PrepareUpdate(const TString& path, const TValue& value)
    {
        return TSetUpdate{
            .Path = path,
            .Payload = BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer).Value(value);
            }))
        };
    }

    template <typename TMeta>
    TObjectIdentity RemoveUuidFromMeta(const TObjectIdentity& identity)
    {
        THROW_ERROR_EXCEPTION_IF(!std::holds_alternative<TPayload>(identity),
            "Expected meta object identity");
        auto payload = std::get<TPayload>(identity);
        TPayload result;
        Visit(payload,
            [&] (const TProtobufPayload& payload) {
                TMeta meta;
                ASSERT_TRUE(meta.ParseFromString(payload.Protobuf));
                meta.clear_uuid();
                meta.clear_fqid();
                result = TProtobufPayload{
                    .Protobuf = meta.SerializeAsString()
                };
            },
            [] (const TYsonPayload&) {
                THROW_ERROR_EXCEPTION("Removing uuid form yson payload is not supported yet");
            },
            [] (const TNullPayload&) {
                THROW_ERROR_EXCEPTION("Expected non-empty payload to remove uuid");
            });

        return result;
    }
};

TEST_F(TNativeSelectObjectHistoryTestSuite, DistinctBy)
{
    auto id = CreateBook(
        Client_,
        PublisherId,
        /*bookId*/ RandomNumber<ui64>(1'000'000'000),
        /*released*/ false);
    id = RemoveUuidFromMeta<NProto::NDataModel::TBookMeta>(id);
    auto timestamp = GenerateTimestamp();

    UpdateBook(id, /*patch*/ {
        PrepareUpdate("/spec/font", "Times"),
        PrepareUpdate("/spec/year", 1998)
    });
    ASSERT_EQ(1u, SelectBookHistory(id, timestamp).Events.size());

    UpdateBook(id, /*patch*/ {
        PrepareUpdate("/spec/font", "Times"),
        PrepareUpdate("/spec/year", 1998)
    });
    ASSERT_EQ(1u, SelectBookHistory(id, timestamp).Events.size());

    UpdateBook(id, /*patch*/ {
        PrepareUpdate("/spec/font", "Times"),
        PrepareUpdate("/spec/year", 1999)
    });
    ASSERT_EQ(2u, SelectBookHistory(id, timestamp).Events.size());
    ASSERT_EQ(1u, SelectBookHistory(id, timestamp, /*distinctBy*/ TAttributeSelector{"/spec/font"}).Events.size());
    ASSERT_EQ(2u, SelectBookHistory(id, timestamp, /*distinctBy*/ TAttributeSelector{"/spec/year"}).Events.size());
}

////////////////////////////////////////////////////////////////////////////////

class TNativeReadUncommittedTestSuite
    : public TNativeClientTestSuite
{ };

TEST_F(TNativeReadUncommittedTestSuite, MasterDispatch)
{
    auto transaction = WaitFor(Client_->StartTransaction())
        .ValueOrThrow();

    auto publisherId = RandomNumber<ui64>(1000000000);
    CreatePublisher(Client_, publisherId, transaction.TransactionId);

    auto object = WaitFor(Client_->GetObject(
        ToString(publisherId),
        TObjectTypeValues::Publisher,
        {"/spec/name"},
        TGetObjectOptions{.TransactionId=transaction.TransactionId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(object.Result.ValuePayloads), 1);

    WaitFor(Client_->CommitTransaction(transaction.TransactionId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
