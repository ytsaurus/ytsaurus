#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature/cypress_key_store.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/key_info.h>
#include <yt/yt/server/lib/signature/key_store.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/library/safe_assert/safe_assert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace std::chrono_literals;

using ::testing::_;
using ::testing::AllOf;
using ::testing::Field;
using ::testing::HasSubstr;
using ::testing::Pointee;
using ::testing::Pointer;
using ::testing::ResultOf;
using ::testing::Return;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<TMockClient>;
DEFINE_REFCOUNTED_TYPE(TStrictMockClient)

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyReaderTest
    : public ::testing::Test
{
    TIntrusivePtr<TStrictMockClient> Client = New<TStrictMockClient>();

    TOwnerId OwnerId = TOwnerId("test");
    TKeyId KeyId = TKeyId(TGuid(1, 2, 3, 4));
    TInstant NowTime = Now();
    TInstant ExpiresAt = NowTime + TDuration::Hours(10);
    TKeyPairMetadata Meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId,
        .KeyId = KeyId,
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = ExpiresAt,
    };
    TCypressKeyReaderConfigPtr Config = New<TCypressKeyReaderConfig>();

    TCypressKeyReaderTest()
    {
        Config->CypressReadOptions->ReadFrom = EMasterChannelKind::LocalCache;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKey)
{
    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);
    EXPECT_CALL(*Client, GetNode(
        "//sys/public_keys/by_owner/test/4-3-2-1",
        Field("ReadFrom", &TGetNodeOptions::ReadFrom, EMasterChannelKind::LocalCache)))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(simpleKeyInfo))));

    auto reader = CreateCypressKeyReader(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_THAT(keyInfo.ValueOrThrow(), Pointee(*simpleKeyInfo));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyNotFound)
{
    EXPECT_CALL(*Client, GetNode(_, _))
        .WillOnce(Return(MakeFuture<TYsonString>(TError("Key not found"))));

    auto reader = CreateCypressKeyReader(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_FALSE(keyInfo.IsOK());
    EXPECT_THAT(keyInfo.GetMessage(), HasSubstr("Key not found"));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyInvalidString)
{
    EXPECT_CALL(*Client, GetNode(_, _))
        .WillOnce(Return(MakeFuture<TYsonString>(ConvertToYsonString("abacaba"))));

    auto reader = CreateCypressKeyReader(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_FALSE(keyInfo.IsOK());
    EXPECT_THAT(keyInfo.GetMessage(), HasSubstr("node has invalid type"));
}

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyWriterNoInitTest
    : public ::testing::Test
{
    TIntrusivePtr<TStrictMockClient> Client = New<TStrictMockClient>();

    TOwnerId OwnerId = TOwnerId("test");
    TInstant NowTime = Now();
    TInstant ExpiresAt = NowTime + TDuration::Hours(10);
    TKeyPairMetadata Meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId,
        .KeyId = TKeyId(TGuid(1, 2, 3, 4)),
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = ExpiresAt,
    };
    TCypressKeyWriterConfigPtr Config = New<TCypressKeyWriterConfig>();

    TCypressKeyWriterNoInitTest()
    {
        Config->OwnerId = OwnerId;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyWriterTest
    : public TCypressKeyWriterNoInitTest
{
    TCypressKeyWriterTest()
    {
        auto optionMatcher = Field("IgnoreExisting", &TCreateNodeOptions::IgnoreExisting, true);
        EXPECT_CALL(*Client, CreateNode("//sys/public_keys/by_owner/test", EObjectType::MapNode, optionMatcher))
            .WillOnce(Return(MakeFuture(TNodeId())));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, Init)
{
    auto writer = CreateCypressKeyWriter(Config, Client);
    EXPECT_TRUE(WaitFor(writer).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterNoInitTest, InitFailed)
{
    EXPECT_CALL(*Client, CreateNode("//sys/public_keys/by_owner/test", _, _))
        .WillOnce(Return(MakeFuture<TNodeId>(TError("failure"))));

    auto writer = WaitFor(CreateCypressKeyWriter(Config, Client));

    EXPECT_FALSE(writer.IsOK());
    EXPECT_THAT(writer.GetMessage(), HasSubstr("failure"));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, GetOwner)
{
    auto writer = WaitFor(CreateCypressKeyWriter(Config, Client))
        .ValueOrThrow();

    EXPECT_EQ(writer->GetOwner(), OwnerId);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, RegisterKey)
{
    auto optionMatcher = AllOf(
        Field("IgnoreExisting", &TCreateNodeOptions::IgnoreExisting, false),
        Field("Attributes", &TCreateNodeOptions::Attributes, Pointer(ResultOf([] (auto attributes) {
            return attributes->template Get<TInstant>("expiration_time");
        }, ExpiresAt + Config->KeyDeletionDelay))));

    EXPECT_CALL(*Client, CreateNode("//sys/public_keys/by_owner/test/4-3-2-1", EObjectType::Document, optionMatcher))
        .WillOnce(Return(MakeFuture(TNodeId())));

    auto keyInfo = New<TKeyInfo>(TPublicKey{}, Meta);

    EXPECT_CALL(*Client, SetNode("//sys/public_keys/by_owner/test/4-3-2-1", ConvertToYsonString(keyInfo), _))
        .WillOnce(Return(VoidFuture));

    auto writer = WaitFor(CreateCypressKeyWriter(Config, Client))
        .ValueOrThrow();

    EXPECT_TRUE(WaitFor(writer->RegisterKey(keyInfo)).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, RegisterKeyFailed)
{
    EXPECT_CALL(*Client, CreateNode("//sys/public_keys/by_owner/test/4-3-2-1", _, _))
        .WillOnce(Return(MakeFuture<TNodeId>(TError("some error"))));

    auto writer = WaitFor(CreateCypressKeyWriter(Config, Client))
        .ValueOrThrow();

    auto keyInfo = New<TKeyInfo>(TPublicKey{}, Meta);

    auto error = WaitFor(writer->RegisterKey(keyInfo));
    EXPECT_FALSE(error.IsOK());
    EXPECT_THAT(error.GetMessage(), HasSubstr("some error"));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, RegisterKeyWrongOwner)
{
    auto writer = WaitFor(CreateCypressKeyWriter(Config, Client))
        .ValueOrThrow();

    // TODO(pavook): enable this test when we can replace YT_VERIFY with exceptions.
    if (false) {
        auto metaNotMine = std::visit([] (const auto& meta) {
            auto ret = meta;
            ret.OwnerId = TOwnerId("notMine");
            return ret;
        }, Meta);
        auto notMineKeyInfo = New<TKeyInfo>(TPublicKey{}, metaNotMine);

        EXPECT_THROW(std::ignore = WaitFor(writer->RegisterKey(notMineKeyInfo)), TAssertionFailedException);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
