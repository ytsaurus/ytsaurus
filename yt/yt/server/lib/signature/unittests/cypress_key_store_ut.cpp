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
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyReaderTest, FindKeyNotFound)
{
    EXPECT_CALL(*Client, GetNode(_, _))
        .WillOnce(Return(MakeFuture<TYsonString>(TError("Key not found"))));

    auto reader = New<TCypressKeyReader>(Config, Client);

    auto keyInfo = WaitFor(reader->FindKey(OwnerId, KeyId));
    EXPECT_FALSE(keyInfo.IsOK());
    EXPECT_THAT(keyInfo.GetMessage(), HasSubstr("Key not found"));
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

} // namespace
} // namespace NYT::NSignature
