#include "yc_authenticator.h"

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

constinit const auto Logger = AuthLogger;

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

class TYCAuthenticatorBase
    : public virtual TRefCounted
{
protected:
    const TYCAuthenticatorConfigPtr Config_;
    const NHttp::IRetryingClientPtr HttpClient_;
    const ICypressUserManagerPtr UserManager_;
    TCounter Calls_;
    TCounter CallErrors_;
    TEventTimer CallTime_;

    TYCAuthenticatorBase(
        TYCAuthenticatorConfigPtr config,
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
        , Calls_(profiler.Counter("/yc_calls"))
        , CallErrors_(profiler.Counter("/yc_call_server_errors"))
        , CallTime_(profiler.Timer("/yc_call_time"))
    { }

    // Returns json tag name for authenticate request (`iam-token` or `yc-session-cookie`).
    virtual TStringBuf GetAuthTagName() const = 0;

    // Returns authenticator name for logs.
    virtual TStringBuf GetLogName() const = 0;

    virtual TStringBuf GetRealm() const = 0;

    TAuthenticationResult DoAuthenticate(const std::string& tokenOrCookie, TGuid callId)
    {
        TSafeUrlBuilder builder;
        builder.AppendString(Format("%v://%v:%v/authenticate",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port));

        auto realUrl = builder.FlushRealUrl();
        auto safeUrl = builder.FlushSafeUrl();

        static const auto retryChecker = BIND([] (const TError& error) {
            return error.FindMatching(EErrorCode::YCRetryableServerError).has_value();
        });

        auto jsonResponseChecker = CreateJsonResponseChecker(
            New<TJsonFormatConfig>(),
            BIND(&TYCAuthenticatorBase::DoCheckYCServiceResponse, MakeStrong(this), callId),
            retryChecker);

        YT_LOG_DEBUG(
            "Calling YC %v authentication service to get user info (Url: %v, CallId: %v)",
            GetLogName(),
            safeUrl,
            callId);

        TStringStream outputStream;
        auto writer = CreateJsonWriter(&outputStream);

        BuildYsonFluently(writer.get())
            .BeginMap()
                .Item(GetAuthTagName()).Value(tokenOrCookie)
            .EndMap();

        writer->Flush();

        auto body = TSharedRef::FromString(outputStream.Str());

        auto headers = New<THeaders>();
        headers->Add("Content-Type", "application/json");

        Calls_.Increment();
        TWallTimer timer;

        auto result = WaitFor(HttpClient_->Post(
            jsonResponseChecker,
            realUrl,
            body,
            headers));

        CallTime_.Record(timer.GetElapsedTime());

        if (!result.IsOK()) {
            auto error = TError(NRpc::EErrorCode::InvalidCredentials, "YC authentication call failed")
                << result
                << TErrorAttribute("call_id", callId);
            YT_LOG_WARNING(error);
            THROW_ERROR(error);
        }

        const auto& formattedResponse = jsonResponseChecker->GetFormattedResponse()->AsMap();
        auto login = formattedResponse->GetChildValueOrThrow<TString>(Config_->AuthenticateLoginField);

        YT_LOG_DEBUG(
            "YC authenticated (Login: %v, CallId: %v)",
            login,
            callId);

        if (Config_->CheckUserExists) {
            auto error = EnsureUserExists(
                Config_->CreateUserIfNotExists,
                UserManager_,
                login,
                Config_->DefaultUserTags);

            if (!error.IsOK()) {
                YT_LOG_WARNING(error, "Failed to ensure YC user existence (Name: %v, CallId: %v)", login, callId);
                error <<= TErrorAttribute("call_id", callId);
                THROW_ERROR error;
            }
        }

        return TAuthenticationResult{
            .Login = login,
            .Realm = TString(GetRealm()),
        };
    }

private:
    TError DoCheckYCServiceResponse(TGuid callId, const NHttp::IResponsePtr& rsp, const INodePtr& rspNode) const
    {
        const auto statusCode = rsp->GetStatusCode();

        if (statusCode != EStatusCode::OK) {
            TError error;

            if (IsClientHttpError(statusCode)) {
                switch (statusCode) {
                    case EStatusCode::Unauthorized:
                        error = TError(
                            EErrorCode::InvalidUserCredentials,
                            "Invalid Access: the token provided is incorrect, expired, or has been revoked");
                        break;
                    case EStatusCode::Forbidden:
                        error = TError(
                            EErrorCode::InvalidUserCredentials,
                            "Access is prohibited for this user");
                        break;
                    case EStatusCode::BadRequest:
                    case EStatusCode::RangeNotSatisfiable:
                        error = TError(
                            EErrorCode::YCProtocolError,
                            "Communication issue between YT and YC Iam token authentication service");
                        break;
                    default:
                        error = TError(
                            EErrorCode::UnexpectedClientYCError,
                            "YC authentication service response has non-ok status code %v", static_cast<int>(rsp->GetStatusCode()));
                        YT_LOG_WARNING(
                            "YC %s authentication call attempt failed (CallId: %v, StatusCode: %v)",
                            GetLogName(),
                            callId,
                            statusCode);
                        break;
                }

            } else {
                CallErrors_.Increment();
                YT_LOG_WARNING(
                    "YC %v authentication service response has server error status code (CallId: %v, StatusCode: %v)",
                    GetLogName(),
                    callId,
                    statusCode);

                if (IsRetryableHttpError(statusCode)) {
                    Calls_.Increment();
                    error = TError(
                        EErrorCode::YCRetryableServerError,
                        "YC authentication service response has non-ok status code %v", static_cast<int>(rsp->GetStatusCode()));
                } else {
                    error = TError(
                        EErrorCode::UnexpectedServerYCError,
                        "YC authentication service response has non-ok status code %v", static_cast<int>(rsp->GetStatusCode()));
                }
            }

            return error;
        }

        if (rspNode->GetType() != ENodeType::Map) {
            return TError("YC authentication service response content has unexpected node type")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rspNode->GetType());
        }

        auto loginNode = rspNode->AsMap()->FindChild(Config_->AuthenticateLoginField);
        if (!loginNode || loginNode->GetType() != ENodeType::String) {
            return TError("YC authentication service response content has no login field or login node type is unexpected")
                << TErrorAttribute("login_field", Config_->AuthenticateLoginField);
        }

        return {};
    }

    bool IsRetryableHttpError(const NHttp::EStatusCode& code) const
    {
        if (Config_->RetryAllServerErrors && IsServerHttpError(code)) {
            return true;
        }
        const auto& retryableStatusCodes = Config_->RetryStatusCodes;
        if (std::find(retryableStatusCodes.begin(), retryableStatusCodes.end(), static_cast<i64>(code)) != retryableStatusCodes.end()) {
            return true;
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYCTokenAuthenticator
    : public TYCAuthenticatorBase
    , public ITokenAuthenticator
{
public:
    TYCTokenAuthenticator(
        TYCAuthenticatorConfigPtr config,
        IPollerPtr poller,
        ICypressUserManagerPtr userManager,
        TProfiler profiler)
        : TYCAuthenticatorBase(
            std::move(config),
            std::move(poller),
            std::move(userManager),
            std::move(profiler))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        auto callId = TGuid::Create();
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenHash = GetCryptoHash(token);

        YT_LOG_DEBUG(
            "Authenticating user with YC IAM token (TokenHash: %v, UserIP: %v, CallId: %v)",
            tokenHash,
            userIP,
            callId);

        return BIND(&TYCTokenAuthenticator::DoAuthenticate, MakeStrong(this), token, callId)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

protected:
    TStringBuf GetAuthTagName() const override
    {
        return "iam-token";
    }

    TStringBuf GetLogName() const override
    {
        return "IAM token";
    }

    TStringBuf GetRealm() const override
    {
        return YCIamTokenRealm;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYCCookieAuthenticator
    : public TYCAuthenticatorBase
    , public ICookieAuthenticator
{
public:
    TYCCookieAuthenticator(
        TYCAuthenticatorConfigPtr config,
        IPollerPtr poller,
        ICypressUserManagerPtr userManager,
        TProfiler profiler)
        : TYCAuthenticatorBase(
            std::move(config),
            std::move(poller),
            std::move(userManager),
            std::move(profiler))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        auto callId = TGuid::Create();
        const auto& cookies = credentials.Cookies;
        auto cookie = GetOrCrash(cookies, YCSessionCookieName);
        const auto& userIP = credentials.UserIP;
        auto cookieHash = GetCryptoHash(cookie);

        YT_LOG_DEBUG(
            "Authenticating user with YC session cookie (CookieHash: %v, UserIP: %v, CallId: %v)",
            cookieHash,
            userIP,
            callId);

        return BIND(&TYCCookieAuthenticator::DoAuthenticate, MakeStrong(this), cookie, callId)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            YCSessionCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(YCSessionCookieName);
    }

protected:
    TStringBuf GetAuthTagName() const override
    {
        return "yc-session-cookie";
    }

    TStringBuf GetLogName() const override
    {
        return "yc_session cookie";
    }

    TStringBuf GetRealm() const override
    {
        return YCSessionCookieRealm;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateYCIamTokenAuthenticator(
    TYCAuthenticatorConfigPtr config,
    IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    TProfiler profiler)
{
    return New<TYCTokenAuthenticator>(
        std::move(config),
        std::move(poller),
        std::move(userManager),
        std::move(profiler));
}

ICookieAuthenticatorPtr CreateYCSessionCookieAuthenticator(
    TYCAuthenticatorConfigPtr config,
    IPollerPtr poller,
    ICypressUserManagerPtr userManager,
    TProfiler profiler)
{
    return New<TYCCookieAuthenticator>(
        std::move(config),
        std::move(poller),
        std::move(userManager),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
