#include <yt/orm/example/client/proto/data_model/autogen/schema.pb.h>

#include <yt/yt/orm/example/client/misc/autogen/traits.h>

#include <yt/yt/orm/client/objects/key.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>
#include <library/cpp/testing/gtest_protobuf/matcher.h>

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

using NOrm::NClient::NObjects::TObjectKey;

////////////////////////////////////////////////////////////////////////////////

TEST(TObjectTraitsTest, TestObjectKeyGetters)
{
    NProto::NDataModel::TPublisherMeta meta;

    EXPECT_THROW(NApi::GetObjectKey(meta, true), std::exception);
    meta.set_creation_time(42);
    EXPECT_THROW(NApi::GetObjectKey(meta, true), std::exception);
    TObjectKey key = NApi::GetObjectKey(meta, false);
    EXPECT_EQ(key, TObjectKey(0));
    meta.set_id(5);
    key = NApi::GetObjectKey(meta, false);
    EXPECT_EQ(key, TObjectKey(5));
    key = NApi::GetObjectKey(meta, true);
    EXPECT_EQ(key, TObjectKey(5));
    meta.set_id(0);
    key = NApi::GetObjectKey(meta, false);
    EXPECT_EQ(key, TObjectKey(0));

    NProto::NDataModel::TBookMeta bookMeta;

    bookMeta.set_id(5);
    key = NApi::GetObjectKey(bookMeta, false);
    EXPECT_EQ(key.GetWithDefault<i64>(0), 5);
    EXPECT_THROW(NApi::GetObjectKey(bookMeta, true), std::exception);
    bookMeta.set_id2(2);
    key = NApi::GetObjectKey(bookMeta, true);
    EXPECT_EQ(key, TObjectKey(5, 2));

    EXPECT_THROW(NApi::GetParentKey(bookMeta, true), std::exception);
    bookMeta.set_publisher_id(11);
    TObjectKey parentKey = NApi::GetParentKey(bookMeta, true);
    EXPECT_EQ(parentKey, TObjectKey(11));
    EXPECT_EQ(NApi::GetPrimaryKey(bookMeta, true), parentKey + key);

    NProto::NDataModel::TBufferedTimestampIdMeta timestampIdMeta;
    EXPECT_THROW(NApi::GetObjectKey(timestampIdMeta, true), std::exception);
    timestampIdMeta.set_i64_id(1);
    EXPECT_THROW(NApi::GetObjectKey(timestampIdMeta, true), std::exception);
    timestampIdMeta.clear_i64_id();
    timestampIdMeta.set_ui64_id(1u);
    EXPECT_THROW(NApi::GetObjectKey(timestampIdMeta, true), std::exception);
    timestampIdMeta.set_i64_id(1);
    key = NApi::GetObjectKey(timestampIdMeta, true);
    EXPECT_EQ(key, TObjectKey(timestampIdMeta.i64_id(), timestampIdMeta.ui64_id()));
}

TEST(TObjectTraitsTest, TestObjectKeySettersSimple)
{
    const TObjectKey expectedBookKey{11, 2};

    NProto::NDataModel::TBookMeta bookMeta;
    EXPECT_NO_THROW(NApi::SetObjectKey(bookMeta, expectedBookKey));
    EXPECT_EQ(expectedBookKey.GetWithDefault<i64>(0), bookMeta.id());
    EXPECT_EQ(expectedBookKey.GetWithDefault<i64>(1), bookMeta.id2());

    const auto expectedPublisherId = 12;
    const TObjectKey publisherKey{expectedPublisherId};
    EXPECT_NO_THROW(NApi::TrySetParentKey(bookMeta, publisherKey));
    EXPECT_EQ(expectedPublisherId, bookMeta.publisher_id());
}

TEST(TObjectTraitsTest, TestObjectKeySettersSimpleNullify)
{
    NProto::NDataModel::TBookMeta bookMeta;
    bookMeta.set_id(11);
    bookMeta.set_id2(2);
    EXPECT_NO_THROW(NApi::SetObjectKey(bookMeta, TObjectKey{}));
    EXPECT_EQ({}, bookMeta.id());
    EXPECT_EQ({}, bookMeta.id2());

    bookMeta.set_publisher_id(11);
    EXPECT_NO_THROW(NApi::TrySetParentKey(bookMeta, TObjectKey{}));
    EXPECT_EQ({}, bookMeta.publisher_id());
}

TEST(TObjectTraitsTest, TestObjectKeySettersComposite)
{
    NProto::NDataModel::TBufferedTimestampIdMeta timestampIdMeta;
    const auto expectedI64 = 12;
    const auto expectedUI64 = 13ul;

    TObjectKey fullKey{expectedI64, expectedUI64};
    EXPECT_NO_THROW(NApi::SetObjectKey(timestampIdMeta, fullKey));
    EXPECT_EQ(expectedI64, timestampIdMeta.i64_id());
    EXPECT_EQ(expectedUI64, timestampIdMeta.ui64_id());

    TObjectKey partialKey{expectedI64};
    EXPECT_ANY_THROW(NApi::SetObjectKey(timestampIdMeta, partialKey));

    TObjectKey emptyKey{};
    EXPECT_NO_THROW(NApi::SetObjectKey(timestampIdMeta, emptyKey));
    EXPECT_EQ({}, timestampIdMeta.i64_id());
    EXPECT_EQ({}, timestampIdMeta.ui64_id());
}

TEST(TObjectTraitsTest, TestGetReferences)
{
    NProto::NDataModel::TBook book;
    book.mutable_spec()->set_editor_id("Gogol");
    book.mutable_spec()->mutable_author_ids()->Add("Saltykov");
    auto references = NApi::GetReferences(book);
    EXPECT_EQ(4u, references.size());
    {
        const auto key = std::pair{NClient::NApi::EObjectType::Editor, NYT::NYPath::TYPath{"/spec/editor_id"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(1u, referencedKeys.size());
        NProto::NDataModel::TSomeMeta meta;
        meta.set_id("Gogol");
        auto expectedKey = NApi::GetObjectKey(meta, false);
        EXPECT_EQ(expectedKey, referencedKeys.front());
    }
    {
        const auto key = std::pair{NClient::NApi::EObjectType::Author, NYT::NYPath::TYPath{"/spec/author_ids"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(1u, referencedKeys.size());
        NProto::NDataModel::TAuthorMeta meta;
        meta.set_id("Saltykov");
        auto expectedKey = NApi::GetObjectKey(meta, false);
        EXPECT_EQ(expectedKey, referencedKeys.front());
    }
    {
        const auto key = std::pair{NClient::NApi::EObjectType::Illustrator, NYT::NYPath::TYPath{"/spec/cover_illustrator_id"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(1u, referencedKeys.size());
        NProto::NDataModel::TIllustratorMeta meta;
        meta.set_uid(0); // Means empty.
        auto expectedKey = NApi::GetObjectKey(meta, false);
        EXPECT_EQ(expectedKey, referencedKeys.front());
    }
    {
        const auto key = std::pair{NClient::NApi::EObjectType::Illustrator, NYT::NYPath::TYPath{"/spec/illustrator_id"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(1u, referencedKeys.size());
        NProto::NDataModel::TIllustratorMeta meta;
        meta.set_uid(0); // Means empty.
        auto expectedKey = NApi::GetObjectKey(meta, false);
        EXPECT_EQ(expectedKey, referencedKeys.front());
    }
}

TEST(TObjectTraitsTest, TestGetReferencesWithSuffixes)
{
    NProto::NDataModel::THitchhiker hitchhiker;
    hitchhiker.mutable_spec()->mutable_favorite_book()->set_id(14);

    {
        NProto::NDataModel::THitchhikerSpec::TBookId bookId;
        bookId.set_id(15);
        hitchhiker.mutable_spec()->mutable_hated_books()->Add(std::move(bookId));
    }

    for (auto id2 : std::array{16, 17}) {
        NProto::NDataModel::THitchhikerSpec::TBookId bookId;
        bookId.set_id2(id2);
        hitchhiker.mutable_spec()->mutable_hated_books()->Add(std::move(bookId));
    }

    auto references = NApi::GetReferences(hitchhiker);
    EXPECT_EQ(4u, references.size());

    {
        const auto key = std::pair{NClient::NApi::EObjectType::Book, NYT::NYPath::TYPath{"/spec/favorite_book"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(1u, referencedKeys.size());
        NProto::NDataModel::TBookMeta bookMeta;
        bookMeta.set_id(14);
        EXPECT_EQ(NApi::GetObjectKey(bookMeta, false), referencedKeys.front());
    }
    {
        const auto key = std::pair{NClient::NApi::EObjectType::Book, NYT::NYPath::TYPath{"/spec/hated_books"}};
        auto referencedKeys = references[key];
        EXPECT_EQ(3u, referencedKeys.size());

        NProto::NDataModel::TBookMeta meta;
        meta.set_id(15);
        auto expectedKey = NApi::GetObjectKey(meta, false);
        EXPECT_TRUE(expectedKey == referencedKeys[0] || expectedKey == referencedKeys[1] || expectedKey == referencedKeys[2]);

        for (auto id2 : std::array{16, 17}) {
            NProto::NDataModel::TBookMeta bookMeta;
            bookMeta.set_id2(id2);
            auto expectedKey = NApi::GetObjectKey(bookMeta, false);
            EXPECT_TRUE(expectedKey == referencedKeys[0] || expectedKey == referencedKeys[1] || expectedKey == referencedKeys[2]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
