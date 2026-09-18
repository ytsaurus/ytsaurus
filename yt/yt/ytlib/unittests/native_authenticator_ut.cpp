#include <yt/yt/ytlib/auth/config.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/native_authenticator.h>

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NAuth {
namespace {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TFakeTvmService
    : public IDynamicTvmService
{
public:
    static constexpr auto SelfTvmId = TTvmId(1000);

    std::function<TParsedServiceTicket(const std::string&)> OnParseServiceTicket;

    const TTvmServiceConfigPtr& GetConfig() override
    {
        return Config_;
    }

    std::optional<TTvmId> TryGetSelfTvmId() override
    {
        return SelfTvmId;
    }

    TTvmId GetSelfTvmIdOrThrow() override
    {
        return SelfTvmId;
    }

    std::string GetServiceTicket(const std::string& /*serviceAlias*/) override
    {
        YT_UNIMPLEMENTED();
    }

    std::string GetServiceTicket(TTvmId /*serviceId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TParsedTicket ParseUserTicket(const std::string& /*ticket*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TParsedServiceTicket ParseServiceTicket(const std::string& ticket) override
    {
        return OnParseServiceTicket(ticket);
    }

    void AddDestinationServiceIds(const std::vector<TTvmId>& /*serviceIds*/) override
    { }

private:
    const TTvmServiceConfigPtr Config_ = New<TTvmServiceConfig>();
};

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticatorTest
    : public ::testing::Test
{
protected:
    TIntrusivePtr<TFakeTvmService> TvmService_;

    void SetUp() override
    {
        TvmService_ = New<TFakeTvmService>();
        TvmService_->OnParseServiceTicket = [] (const std::string&) {
            return TParsedServiceTicket{.TvmId = TTvmId(2000)};
        };

        TNativeAuthenticationManager::Get()->SetTvmService(TvmService_);
        SetValidationEnabled(true);
    }

    void TearDown() override
    {
        TNativeAuthenticationManager::Get()->SetTvmService(nullptr);
        SetValidationEnabled(false);
    }

    static void SetValidationEnabled(bool enabled)
    {
        auto config = New<TNativeAuthenticationManagerDynamicConfig>();
        config->EnableValidation = enabled;
        TNativeAuthenticationManager::Get()->Reconfigure(config);
    }

    static TErrorOr<NRpc::TAuthenticationResult> Authenticate(const IAuthenticatorPtr& authenticator)
    {
        NRpc::NProto::TRequestHeader header;
        header.set_user("user");
        auto* ext = header.MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_service_ticket("ticket");

        TAuthenticationContext context{
            .Header = &header,
            .IsLocal = false,
        };
        return authenticator->AsyncAuthenticate(context).BlockingGet();
    }
};

TEST_F(TNativeAuthenticatorTest, AcceptedSource)
{
    auto authenticator = CreateNativeAuthenticator([] (TTvmId) {
        return TError();
    });

    auto result = Authenticate(authenticator);
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("user", result.Value().User);
}

TEST_F(TNativeAuthenticatorTest, AuthoritativeRejection)
{
    auto authenticator = CreateNativeAuthenticator([] (TTvmId tvmId) {
        return TError(NRpc::EErrorCode::AuthenticationError, "Source TVM id %v is rejected", tvmId);
    });

    auto result = Authenticate(authenticator);
    ASSERT_FALSE(result.IsOK());
    EXPECT_EQ(NRpc::EErrorCode::AuthenticationError, result.GetCode());
    EXPECT_FALSE(result.FindMatching(NRpc::EErrorCode::TransientFailure));
}

TEST_F(TNativeAuthenticatorTest, TransientRejection)
{
    auto authenticator = CreateNativeAuthenticator([] (TTvmId tvmId) {
        return TError(NRpc::EErrorCode::TransientFailure, "Source TVM id %v is rejected", tvmId);
    });

    auto result = Authenticate(authenticator);
    ASSERT_FALSE(result.IsOK());
    EXPECT_TRUE(result.FindMatching(NRpc::EErrorCode::TransientFailure));
    EXPECT_EQ("Error validating service ticket", result.GetMessage());
}

TEST_F(TNativeAuthenticatorTest, InvalidTicket)
{
    TvmService_->OnParseServiceTicket = [] (const std::string&) -> TParsedServiceTicket {
        THROW_ERROR_EXCEPTION("Invalid ticket");
    };
    auto authenticator = CreateNativeAuthenticator([] (TTvmId) {
        return TError();
    });

    auto result = Authenticate(authenticator);
    ASSERT_FALSE(result.IsOK());
    EXPECT_EQ(NRpc::EErrorCode::AuthenticationError, result.GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
