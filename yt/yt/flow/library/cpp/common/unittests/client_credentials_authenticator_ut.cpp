#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/authenticator.h>

#include <yt/yt/flow/library/cpp/client/authentication.h>

#include <yt/yt/library/tvm/tvm_base.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/connection.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NRpc;

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TClientCredentialsAuthenticatorTest
    : public ::testing::Test
{
protected:
    const TIntrusivePtr<StrictMock<TMockConnection>> Connection_ = New<StrictMock<TMockConnection>>();
    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const IAuthenticatorPtr Authenticator_ = CreateClientCredentialsAuthenticator(Connection_);

    //! Options the authenticator built the cluster client with.
    TClientOptions ClientOptions_;

    void ExpectClientCreation()
    {
        EXPECT_CALL(*Connection_, CreateClient(_))
            .WillOnce([this] (const TClientOptions& options) {
                ClientOptions_ = options;
                return Client_;
            });
    }

    static TAuthenticationContext MakeContext(const NRpc::NProto::TRequestHeader& header)
    {
        return TAuthenticationContext{
            .Header = &header,
            .IsLocal = false,
        };
    }

    //! A header marked as direct; the tests put the credentials on top of it.
    static NRpc::NProto::TRequestHeader MakeDirectHeader()
    {
        NRpc::NProto::TRequestHeader header;
        MarkDirectRequest(&header);
        return header;
    }

    static NRpc::NProto::TRequestHeader MakeHeaderWithToken(const std::string& token)
    {
        auto header = MakeDirectHeader();
        header.MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext)->set_token(token);
        return header;
    }
};

TEST_F(TClientCredentialsAuthenticatorTest, IgnoresForwardedRequests)
{
    NRpc::NProto::TRequestHeader header;
    EXPECT_FALSE(IsDirectRequest(header));
    EXPECT_FALSE(Authenticator_->CanAuthenticate(MakeContext(header)));

    // The RPC proxy names the user and, on a cluster with TVM, carries a service ticket of its
    // own, issued for the controller; the cluster would not accept it as the caller's credentials.
    header.set_user("alice");
    header.MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext)->set_service_ticket("proxy-ticket");
    EXPECT_FALSE(IsDirectRequest(header));
    EXPECT_FALSE(Authenticator_->CanAuthenticate(MakeContext(header)));
}

TEST_F(TClientCredentialsAuthenticatorTest, RejectsDirectRequestWithoutCredentials)
{
    auto header = MakeDirectHeader();
    EXPECT_TRUE(Authenticator_->CanAuthenticate(MakeContext(header)));

    // No client creation expectation: the cluster is not asked about nobody's credentials.
    auto error = Authenticator_->AsyncAuthenticate(MakeContext(header))
        .BlockingGet();
    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NRpc::EErrorCode::AuthenticationError);
}

TEST_F(TClientCredentialsAuthenticatorTest, ResolvesTokenOwner)
{
    auto header = MakeHeaderWithToken("secret");
    header.set_user("alice");
    EXPECT_TRUE(Authenticator_->CanAuthenticate(MakeContext(header)));

    ExpectClientCreation();
    EXPECT_CALL(*Client_, GetCurrentUser(_))
        .WillOnce(Return(MakeFuture(TGetCurrentUserResult{.User = "alice"})));

    auto result = Authenticator_->AsyncAuthenticate(MakeContext(header))
        .BlockingGet()
        .ValueOrThrow();
    EXPECT_EQ(result.User, "alice");
    EXPECT_EQ(result.Realm, "yt-client-credentials");

    EXPECT_EQ(ClientOptions_.Token, "secret");
    EXPECT_EQ(ClientOptions_.User, "alice");
}

TEST_F(TClientCredentialsAuthenticatorTest, ForwardsServiceTicket)
{
    auto header = MakeDirectHeader();
    header.MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext)->set_service_ticket("ticket");
    EXPECT_TRUE(Authenticator_->CanAuthenticate(MakeContext(header)));

    ExpectClientCreation();
    EXPECT_CALL(*Client_, GetCurrentUser(_))
        .WillOnce(Return(MakeFuture(TGetCurrentUserResult{.User = "robot"})));

    auto result = Authenticator_->AsyncAuthenticate(MakeContext(header))
        .BlockingGet()
        .ValueOrThrow();
    EXPECT_EQ(result.User, "robot");

    EXPECT_FALSE(ClientOptions_.Token);
    ASSERT_TRUE(ClientOptions_.ServiceTicketAuth);
    EXPECT_EQ((*ClientOptions_.ServiceTicketAuth)->IssueServiceTicket(), "ticket");
}

TEST_F(TClientCredentialsAuthenticatorTest, ForwardsUserTicket)
{
    auto header = MakeDirectHeader();
    header.MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext)->set_user_ticket("ticket");
    EXPECT_TRUE(Authenticator_->CanAuthenticate(MakeContext(header)));

    ExpectClientCreation();
    EXPECT_CALL(*Client_, GetCurrentUser(_))
        .WillOnce(Return(MakeFuture(TGetCurrentUserResult{.User = "alice"})));

    auto result = Authenticator_->AsyncAuthenticate(MakeContext(header))
        .BlockingGet()
        .ValueOrThrow();
    EXPECT_EQ(result.User, "alice");

    EXPECT_FALSE(ClientOptions_.Token);
    EXPECT_FALSE(ClientOptions_.ServiceTicketAuth);
    EXPECT_EQ(ClientOptions_.UserTicket, "ticket");
}

TEST_F(TClientCredentialsAuthenticatorTest, RejectsCredentialsTheClusterRejects)
{
    auto header = MakeHeaderWithToken("forged");

    ExpectClientCreation();
    EXPECT_CALL(*Client_, GetCurrentUser(_))
        .WillOnce(Return(MakeFuture<TGetCurrentUserResult>(TError("Invalid token"))));

    auto error = Authenticator_->AsyncAuthenticate(MakeContext(header))
        .BlockingGet();
    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NRpc::EErrorCode::AuthenticationError);
    EXPECT_TRUE(error.FindMatching([] (const TError& inner) {
        return inner.GetMessage() == "Invalid token";
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
