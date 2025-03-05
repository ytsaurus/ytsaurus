#include "yc_iam_token_authenticator.h"

#include "config.h"
#include "credentials.h"
#include "cypress_user_manager.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/retrying_client.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <util/digest/multi.h>


namespace NYT::NAuth {

using namespace NApi;
using namespace NConcurrency;
using namespace NHttp;
using namespace NJson;
using namespace NNet;
using namespace NProfiling;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsClientHttpError(const NHttp::EStatusCode& code)
{
    return static_cast<int>(code) >= 400 && static_cast<int>(code) < 500;
}

bool IsServerHttpError(const NHttp::EStatusCode& code)
{
    return static_cast<int>(code) >= 400 && static_cast<int>(code) < 500;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TYCIAMTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TYCIAMTokenAuthenticator(
        TYCIAMTokenAuthenticatorConfigPtr config,
        IPollerPtr poller,
        ICypressUserManagerPtr userManager,
        TProfiler profiler)
        : Config_(std::move(config))
        , HttpClient_(
            CreateRetryingClient(
                Config_->RetryingClient,
                Config_->Secure
                    ? NHttps::CreateClient(Config_->HttpClient, poller)
                    : NHttp::CreateClient(Config_->HttpClient, poller),
                poller->GetInvoker()))
        , UserManager_(std::move(userManager))
        , YCIAMCalls_(profiler.Counter("/yc_iam_calls"))
        , YCIAMCallErrors_(profiler.Counter("/yc_iam_call_server_errors"))
        , YCIAMCallTime_(profiler.Timer("/yc_iam_call_time"))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        auto callId = TGuid::Create();
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;;
        auto tokenHash = GetCryptoHash(token);

        YT_LOG_DEBUG(
            "Authenticating user with YC IAM token (TokenHash: %v, UserIP: %v, CallId: %v)",
            tokenHash,
            userIP,
            callId);

        return BIND(&TYCIAMTokenAuthenticator::DoAuthenticate, MakeStrong(this), token, callId)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

private:
    const TYCIAMTokenAuthenticatorConfigPtr Config_;
    const NHttp::IRetryingClientPtr HttpClient_;
    const ICypressUserManagerPtr UserManager_;

    TCounter YCIAMCalls_;
    TCounter YCIAMCallErrors_;
    TEventTimer YCIAMCallTime_;

    void CheckAndCreateUser(const std::string& login, TGuid callId)
    {
        auto result = WaitFor(UserManager_->CheckUserExists(TString(login)));
        if (!result.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to verify if user exists")
                << TErrorAttribute("name", login)
                << TErrorAttribute("call_id", callId)
                << std::move(result);
        } else if (!result.Value()) {
            if (Config_->CreateUserIfNotExists) {
                auto result = WaitFor(UserManager_->CreateUser(TString(login)));
                if (!result.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to create user")
                        << TErrorAttribute("name", login)
                        << TErrorAttribute("call_id", callId)
                        << std::move(result);
                }
            } else {
                YT_LOG_DEBUG("User does not exist (Name: %v)", login);
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::InvalidCredentials, "User does not exist")
                    << TErrorAttribute("call_id", callId)
                    << TErrorAttribute("name", login);
            }
        }
    }

    TAuthenticationResult DoAuthenticate(const std::string& token, TGuid callId)
    {
        TSafeUrlBuilder builder;
        builder.AppendString(Format("%v://%v:%v/authenticate",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port));

        auto realUrl = builder.FlushRealUrl();
        auto safeUrl = builder.FlushSafeUrl();

        const static auto retryChecker = BIND([] (const TError& error) {
            return error.FindMatching(EErrorCode::YCIAMRetryableServerError).has_value();
        });

        auto jsonResponseChecker = CreateJsonResponseChecker(
            New<TJsonFormatConfig>(),
            BIND(&TYCIAMTokenAuthenticator::DoCheckYCIAMServiceResponse, MakeStrong(this), callId),
            retryChecker);

        YT_LOG_DEBUG(
            "Calling YC IAM token authentication service to get user info (Url: %v, CallId: %v)",
            safeUrl,
            callId);

        TStringStream outputStream;
        auto writer = CreateJsonWriter(&outputStream);

        BuildYsonFluently(writer.get())
            .BeginMap()
                .Item("iam-token").Value(token)
            .EndMap();

        writer->Flush();

        auto body = TSharedRef::FromString(outputStream.Str());

        auto headers = New<THeaders>();
        headers->Add("Content-Type", "application/json");

        YCIAMCalls_.Increment();
        TWallTimer timer;

        auto result = WaitFor(HttpClient_->Post(
            jsonResponseChecker,
            realUrl,
            body,
            headers));

        YCIAMCallTime_.Record(timer.GetElapsedTime());

        if (!result.IsOK()) {
            auto error = TError(NRpc::EErrorCode::InvalidCredentials, "YC IAM token authentication call failed")
                << result
                << TErrorAttribute("call_id", callId);
            YT_LOG_WARNING(error);
            THROW_ERROR(error);
        }

        const auto& formattedResponse = jsonResponseChecker->GetFormattedResponse()->AsMap();
        auto login = formattedResponse->GetChildValueOrThrow<TString>(Config_->AuthenticateLoginField);

        if (Config_->CheckUserExists) {
            CheckAndCreateUser(login, callId);
        }

        return TAuthenticationResult{
            .Login = login,
            .Realm = TString(YCIAMTokenRealm),
        };
    }

    TError DoCheckYCIAMServiceResponse(TGuid callId, const NHttp::IResponsePtr& rsp, const INodePtr& rspNode) const
    {
        const auto statusCode = rsp->GetStatusCode();

        if (statusCode != EStatusCode::OK) {
            TError error;

            if (IsClientHttpError(statusCode)) {
                switch (statusCode) {
                    case EStatusCode::Unauthorized:
                        error = TError(
                            EErrorCode::InvalidUserCredentials,
                            "Invalid Access: The token provided is incorrect, expired, or has been revoked");
                        break;
                    case EStatusCode::Forbidden:
                        error = TError(
                            EErrorCode::InvalidUserCredentials,
                            "Access is prohibited for this user");
                        break;
                    case EStatusCode::BadRequest:
                    case EStatusCode::RangeNotSatisfiable:
                        error = TError(
                            EErrorCode::YCIAMProtocolError,
                            "Communication issue between YT and YC IAM token authentication service");
                        break;
                    default:
                        error = TError(
                            EErrorCode::UnexpectedClientYCIAMError,
                            "YC IAM token authentication service response has non-ok status code: %v", static_cast<int>(rsp->GetStatusCode()));
                        YT_LOG_WARNING(
                            "YC IAM token authentication call attempt failed (CallId: %v, StatusCode: %v)",
                            callId,
                            statusCode);
                        break;
                }

            } else {
                YCIAMCallErrors_.Increment();
                YT_LOG_WARNING(
                    "YC IAM token authentication service response has server error status code (CallId: %v, StatusCode: %v)",
                    callId,
                    statusCode);

                if (IsRetryableHttpError(statusCode)) {
                    YCIAMCalls_.Increment();
                    error = TError(
                        EErrorCode::YCIAMRetryableServerError,
                        "YC IAM token authentication service response has non-ok status code: %v", static_cast<int>(rsp->GetStatusCode()));
                } else {
                    error = TError(
                        EErrorCode::UnexpectedServerYCIAMError,
                        "YC IAM token authentication service response has non-ok status code: %v", static_cast<int>(rsp->GetStatusCode()));
                }
            }

            return error;
        }

        if (rspNode->GetType() != ENodeType::Map) {
            return TError("YC IAM token authentication service response content has unexpected node type")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rspNode->GetType());
        }

        auto loginNode = rspNode->AsMap()->FindChild(Config_->AuthenticateLoginField);
        if (!loginNode || loginNode->GetType() != ENodeType::String) {
            return TError("YC IAM token authentication service response content has no login field or login node type is unexpected")
                << TErrorAttribute("login_field", Config_->AuthenticateLoginField);
        }

        return {};
    }

    bool IsRetryableHttpError(const NHttp::EStatusCode& code) const
    {
        if (Config_->RetryAllServerErrors && IsServerHttpError(code)) {
            return true;
        }
        const auto& retryStatusCodes = Config_->RetryStatusCodes;
        if (std::find(retryStatusCodes.begin(), retryStatusCodes.end(), static_cast<i64>(code)) != retryStatusCodes.end()) {
            return true;
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateYCIAMTokenAuthenticator(
    TYCIAMTokenAuthenticatorConfigPtr config,
    IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    TProfiler profiler)
{
    return New<TYCIAMTokenAuthenticator>(
        std::move(config),
        std::move(poller),
        std::move(userManager),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
