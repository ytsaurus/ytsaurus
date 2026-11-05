#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/signature/validation/cypress_key_reader.h>

#include <yt/yt/library/signature/validation/config.h>
#include <yt/yt/library/signature/common/key_info.h>
#include <yt/yt/library/signature/common/key_store.h>

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

using ::testing::AllOf;
using ::testing::Field;
using ::testing::HasSubstr;
using ::testing::InSequence;
using ::testing::Pointee;
using ::testing::Pointer;
using ::testing::ResultOf;
using ::testing::Return;
using ::testing::StrictMock;
using ::testing::_;

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
        Config->CypressReadOptions->ReadFrom = EMasterChannelKind::ClientSideCache;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKey)
{
    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);
    EXPECT_CALL(*Client, GetNode(
        "//sys/public_keys/by_owner/test/4-3-2-1",
        Field("ReadFrom", &TGetNodeOptions::ReadFrom, EMasterChannelKind::ClientSideCache)))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(simpleKeyInfo))));

    auto reader = New<TCypressKeyReader>(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_THAT(keyInfo.ValueOrThrow(), Pointee(*simpleKeyInfo));

    auto newConfig = New<TCypressKeyReaderConfig>();
    newConfig->Path = "//sys/public_keys/lawn_mower";
    newConfig->CypressReadOptions->ReadFrom = EMasterChannelKind::Follower;
    reader->Reconfigure(newConfig);

    EXPECT_CALL(*Client, GetNode(
        "//sys/public_keys/lawn_mower/test/4-3-2-1",
        Field("ReadFrom", &TGetNodeOptions::ReadFrom, EMasterChannelKind::Follower)))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(simpleKeyInfo))));

    auto keyInfo2 = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_THAT(keyInfo2.ValueOrThrow(), Pointee(*simpleKeyInfo));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyNotFound)
{
    // NB: A fresh future is minted per call: FindKey consumes its unique result.
    EXPECT_CALL(*Client, GetNode(_, _))
        .Times(2)
        .WillRepeatedly([] (const auto& /*path*/, const auto& /*options*/) {
            return MakeFuture<TYsonString>(TError("Key not found"));
        });

    auto reader = New<TCypressKeyReader>(Config, Client);

    // Errors are not cached: every lookup fetches the key again.
    for (int i = 0; i < 2; ++i) {
        auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
        EXPECT_FALSE(keyInfo.IsOK());
        EXPECT_THAT(keyInfo.GetMessage(), HasSubstr("Key not found"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyInvalidString)
{
    EXPECT_CALL(*Client, GetNode(_, _))
        .WillOnce(Return(MakeFuture<TYsonString>(ConvertToYsonString("abacaba"))));

    auto reader = New<TCypressKeyReader>(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_FALSE(keyInfo.IsOK());
    EXPECT_THAT(keyInfo.GetMessage(), HasSubstr("node has invalid type"));
}


////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, ReconfigureDuringReadOperation)
{
    auto reader = New<TCypressKeyReader>(Config, Client);

    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);

    // We want to test out the case that reconfigure happened during registration.
    // Concurrent reads of the same key are deduplicated, so a single fetch is expected.
    auto barrier = NewPromise<TYsonString>();

    EXPECT_CALL(*Client, GetNode("//sys/public_keys/by_owner/test/4-3-2-1", _))
        .WillOnce(Return(barrier));

    EXPECT_CALL(*Client, GetNode("//sys/shmublic_keys/lawn_mower/test/4-3-2-1", _))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(*simpleKeyInfo))));

    std::vector<TFuture<TKeyInfoPtr>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(reader->FindKey(OwnerId, KeyId));
    }

    auto newConfig = New<TCypressKeyReaderConfig>();
    newConfig->Path = "//sys/shmublic_keys/lawn_mower";
    reader->Reconfigure(newConfig);

    // Release the barrier after reconfigure.
    barrier.Set(ConvertToYsonString(simpleKeyInfo));

    for (int i = 0; i < 10; ++i) {
        futures.push_back(reader->FindKey(OwnerId, KeyId));
    }

    for (auto& future : futures) {
        auto result = WaitFor(future);
        EXPECT_EQ(*result.ValueOrThrow(), *simpleKeyInfo);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyCached)
{
    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);
    EXPECT_CALL(*Client, GetNode("//sys/public_keys/by_owner/test/4-3-2-1", _))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(simpleKeyInfo))));

    auto reader = New<TCypressKeyReader>(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_THAT(keyInfo.ValueOrThrow(), Pointee(*simpleKeyInfo));

    // The second read is served from the cache.
    auto cachedKeyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_THAT(cachedKeyInfo.ValueOrThrow(), Pointee(*simpleKeyInfo));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, ConcurrentFindKeyDeduplicated)
{
    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);

    auto barrier = NewPromise<TYsonString>();
    EXPECT_CALL(*Client, GetNode("//sys/public_keys/by_owner/test/4-3-2-1", _))
        .WillOnce(Return(barrier));

    auto reader = New<TCypressKeyReader>(Config, Client);

    auto firstFuture = reader->FindKey(OwnerId, KeyId);
    auto secondFuture = reader->FindKey(OwnerId, KeyId);
    EXPECT_FALSE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());

    barrier.Set(ConvertToYsonString(simpleKeyInfo));

    EXPECT_THAT(WaitFor(firstFuture).ValueOrThrow(), Pointee(*simpleKeyInfo));
    EXPECT_THAT(WaitFor(secondFuture).ValueOrThrow(), Pointee(*simpleKeyInfo));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindDifferentKeysNotDeduplicated)
{
    auto otherKeyId = TKeyId(TGuid(5, 6, 7, 8));
    TKeyPairMetadata otherMeta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId,
        .KeyId = otherKeyId,
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = ExpiresAt,
    };

    TPublicKey key{};
    auto simpleKeyInfo = New<TKeyInfo>(key, Meta);
    auto otherKeyInfo = New<TKeyInfo>(key, otherMeta);

    EXPECT_CALL(*Client, GetNode("//sys/public_keys/by_owner/test/4-3-2-1", _))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(simpleKeyInfo))));
    EXPECT_CALL(*Client, GetNode("//sys/public_keys/by_owner/test/8-7-6-5", _))
        .WillOnce(Return(MakeFuture(ConvertToYsonString(otherKeyInfo))));

    auto reader = New<TCypressKeyReader>(Config, Client);

    EXPECT_THAT(WaitFor(reader->FindKey(OwnerId, KeyId)).ValueOrThrow(), Pointee(*simpleKeyInfo));
    EXPECT_THAT(WaitFor(reader->FindKey(OwnerId, otherKeyId)).ValueOrThrow(), Pointee(*otherKeyInfo));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
