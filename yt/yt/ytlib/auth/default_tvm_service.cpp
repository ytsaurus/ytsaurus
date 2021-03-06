#include "default_tvm_service.h"
#include "tvm_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/logger.h>

#include <util/system/mutex.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NHttp;
using namespace NYPath;
using namespace NConcurrency;
using namespace NTvmAuth;

using NYT::NLogging::ELogLevel;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

class TTvmLoggerAdapter
    : public NTvmAuth::ILogger
{
protected:
    void Log(int lvl, const TString& msg) override
    {
        ELogLevel ourLvl = ELogLevel::Debug;
        if (lvl < 7) {
            if (lvl >= 5) {
                ourLvl = ELogLevel::Info;
            } else if (lvl == 4) {
                ourLvl = ELogLevel::Warning;
            } else if (lvl == 3) {
                ourLvl = ELogLevel::Error;
            } else {
                ourLvl = ELogLevel::Alert;
            }
        }
        YT_LOG_EVENT(TicketParserLogger_, ourLvl, msg);
    }

    const NLogging::TLogger TicketParserLogger_{"TicketParser"};
};

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmService
    : public ITvmService
{
public:
    TDefaultTvmService(
        TDefaultTvmServiceConfigPtr config,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , HttpClient_(CreateClient(Config_->HttpClient, std::move(poller)))
        , GetServiceTicketCountCounter_(profiler.Counter("/get_service_ticket_count"))
        , GetServiceTicketTimer_(profiler.Timer("/get_service_ticket_time"))
        , SuccessfulGetServiceTicketCountCounter_(profiler.Counter("/successful_get_service_ticket_count"))
        , FailedGetServiceTicketCountCounter_(profiler.Counter("/failed_get_service_ticket_count"))
        , ParseUserTicketCountCounter_(profiler.Counter("/parse_user_ticket_count"))
        , SuccessfulParseUserTicketCountCounter_(profiler.Counter("/successful_parse_user_ticket_count"))
        , FailedParseUserTicketCountCounter_(profiler.Counter("/failed_parse_user_ticket_count"))
        , ClientErrorCountCounter_(profiler.Counter("/client_error_count"))
    {
        if (Config_->ClientEnableUserTicketChecking || Config_->ClientEnableServiceTicketFetching) {
            MakeClient();
        }
    }

    TFuture<TString> GetTicket(const TString& serviceId) override
    {
        if (!Config_->ClientEnableServiceTicketFetching) {
            return GetTicketDeprecated(serviceId);
        }

        YT_LOG_DEBUG("Retrieving TVM ticket (ServiceId: %v)", serviceId);
        GetServiceTicketCountCounter_.Increment();

        try {
            CheckClient();
            // The client caches everything locally, no need for async.
            auto result = Client_->GetServiceTicketFor(serviceId);
            SuccessfulGetServiceTicketCountCounter_.Increment();
            return MakeFuture(result);
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_WARNING(error);
            FailedGetServiceTicketCountCounter_.Increment();
            return MakeFuture<TString>(error);
        }
    }

    TErrorOr<TParsedTicket> ParseUserTicket(const TString& ticket) override
    {
        if (!Config_->ClientEnableUserTicketChecking) {
            return TError("Parsing user tickets disabled");
        }

        YT_LOG_DEBUG("Parsing user ticket: %v", NUtils::RemoveTicketSignature(ticket));
        ParseUserTicketCountCounter_.Increment();

        try {
            CheckClient();
            auto userTicket = Client_->CheckUserTicket(ticket);
            if (!userTicket) {
                THROW_ERROR_EXCEPTION(TString(StatusToString(userTicket.GetStatus())));
            }

            TParsedTicket result;
            result.DefaultUid = userTicket.GetDefaultUid();
            for (const auto& scope : userTicket.GetScopes()) {
                result.Scopes.emplace(scope);
            }

            SuccessfulParseUserTicketCountCounter_.Increment();
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << ex;
            YT_LOG_WARNING(error);
            FailedParseUserTicketCountCounter_.Increment();
            return error;
        }
    }

private:
    const TDefaultTvmServiceConfigPtr Config_;

    const IClientPtr HttpClient_;

    std::unique_ptr<TTvmClient> Client_;

    NProfiling::TCounter GetServiceTicketCountCounter_;
    NProfiling::TEventTimer GetServiceTicketTimer_;
    NProfiling::TCounter SuccessfulGetServiceTicketCountCounter_;
    NProfiling::TCounter FailedGetServiceTicketCountCounter_;

    NProfiling::TCounter ParseUserTicketCountCounter_;
    NProfiling::TCounter SuccessfulParseUserTicketCountCounter_;
    NProfiling::TCounter FailedParseUserTicketCountCounter_;

    NProfiling::TCounter ClientErrorCountCounter_;

private:
    void MakeClient()
    {
        YT_LOG_INFO("Creating TvmClient");

        NTvmApi::TClientSettings settings;
        settings.SetSelfTvmId(Config_->ClientSelfId);
        if (!Config_->ClientDiskCacheDir.empty()) {
            settings.SetDiskCacheDir(Config_->ClientDiskCacheDir);
        }
        if (!Config_->TvmHost.empty() && Config_->TvmPort != 0) {
            settings.SetTvmHostPort(Config_->TvmHost, Config_->TvmPort);
        }
        if (Config_->ClientEnableUserTicketChecking) {
            auto env = FromString<EBlackboxEnv>(Config_->ClientBlackboxEnv);
            settings.EnableUserTicketChecking(env);
        }
        if (Config_->ClientEnableServiceTicketFetching) {
            NTvmApi::TClientSettings::TDstMap dsts;
            for (const auto& [alias, dst] : Config_->ClientDstMap) {
                dsts[alias] = dst;
            }
            settings.EnableServiceTicketsFetchOptions(Config_->ClientSelfSecret, std::move(dsts));
        }

        // If TVM is unreachable _and_ there are no cached keys, this will throw.
        // We'll just crash and restart.
        Client_ = std::make_unique<TTvmClient>(settings, MakeIntrusive<TTvmLoggerAdapter>());
    }

    void CheckClient()
    {
        auto status = Client_->GetStatus();
        switch (status.GetCode()) {
            case TClientStatus::Ok:
                break;
            case TClientStatus::Warning:
                YT_LOG_WARNING("TVM client cache expiring");
                ClientErrorCountCounter_.Increment();
                break;
            default:
                ClientErrorCountCounter_.Increment();
                THROW_ERROR_EXCEPTION(status.GetLastError());
        }
    }

    TFuture<TString> GetTicketDeprecated(const TString& serviceId)
    {
        YT_LOG_DEBUG("Retrieving TVM ticket (ServiceId: %v)",
            serviceId);

        auto headers = MakeRequestHeaders();

        TSafeUrlBuilder builder;
        builder.AppendString(Format("http://%v:%v/tvm/tickets?", Config_->Host, Config_->Port));
        if (!Config_->Src.empty()) {
            builder.AppendParam(TStringBuf("src"), Config_->Src);
            builder.AppendChar('&');
        }
        builder.AppendParam(TStringBuf("dsts"), serviceId);
        builder.AppendString("&format=json");
        auto safeUrl = builder.FlushSafeUrl();
        auto realUrl = builder.FlushRealUrl();

        auto callId = TGuid::Create();

        YT_LOG_DEBUG("Calling TVM daemon (Url: %v, CallId: %v)",
            safeUrl,
            callId);

        GetServiceTicketCountCounter_.Increment();

        NProfiling::TWallTimer timer;
        return HttpClient_->Get(realUrl, headers)
            .WithTimeout(Config_->RequestTimeout)
            .Apply(BIND(
                &TDefaultTvmService::OnTvmCallResult,
                MakeStrong(this),
                callId,
                serviceId,
                timer));
    }

    THeadersPtr MakeRequestHeaders()
    {
        auto headers = New<THeaders>();
        static const TString AuthorizationHeaderName("Authorization");
        headers->Add(AuthorizationHeaderName, Config_->Token);
        return headers;
    }

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    TString OnTvmCallResult(
        TGuid callId,
        const TString& serviceId,
        const NProfiling::TWallTimer& timer,
        const TErrorOr<IResponsePtr>& rspOrError)
    {
        GetServiceTicketTimer_.Record(timer.GetElapsedTime());

        auto onError = [&] (TError error) {
            error.Attributes().Set("call_id", callId);
            FailedGetServiceTicketCountCounter_.Increment();
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        if (!rspOrError.IsOK()) {
            onError(TError(NRpc::EErrorCode::Unavailable, "TVM call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            TErrorCode errorCode = NYT::EErrorCode::Generic;
            int statusCode = static_cast<int>(rsp->GetStatusCode());
            if (statusCode >= 500) {
                errorCode = NRpc::EErrorCode::Unavailable;
            }
            onError(TError(errorCode, "TVM call returned HTTP status code %v", statusCode));
        }

        INodePtr rootNode;
        try {

            YT_LOG_DEBUG("Started reading response body from TVM (CallId: %v)",
            callId);

            auto body = rsp->ReadAll();

            YT_LOG_DEBUG("Finished reading response body from TVM (CallId: %v)\n%v",
                callId,
                body);

            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            static const auto Config = MakeJsonFormatConfig();
            NJson::ParseJson(&stream, builder.get(), Config);
            rootNode = builder->EndTree();

            YT_LOG_DEBUG("Parsed TVM daemon reply (CallId: %v)",
                callId);
        } catch (const std::exception& ex) {
            onError(TError(
                "Error parsing TVM response")
                << ex);
        }

        static const TString ErrorPath("/error");
        auto errorNode = FindNodeByYPath(rootNode, ErrorPath);
        if (errorNode) {
            onError(TError("TVM daemon returned an error")
                << TErrorAttribute("message", errorNode->GetValue<TString>()));
        }

        TString ticket;
        try {
            auto ticketPath = "/" + ToYPathLiteral(serviceId) + "/ticket";
            ticket = GetNodeByYPath(rootNode, ticketPath)->GetValue<TString>();
            SuccessfulGetServiceTicketCountCounter_.Increment();
        } catch (const std::exception& ex) {
            onError(TError("Error parsing TVM daemon reply")
                << ex);
        }

        return ticket;
    }
};

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TDefaultTvmService>(
        std::move(config),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
