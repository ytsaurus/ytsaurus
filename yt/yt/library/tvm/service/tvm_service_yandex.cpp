#include "tvm_service.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/tls_expiring_cache.h>

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/logger.h>
#include <library/cpp/tvmauth/client/misc/api/dynamic_dst/tvm_client.h>
#include <library/cpp/tvmauth/utils.h>

#include <util/system/env.h>

#include <memory>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NConcurrency;
using namespace NTvmAuth;
using namespace NProfiling;

using NYT::NLogging::ELogLevel;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthTvmLogger;

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

NTvmTool::TClientSettings MakeTvmToolSettings(const TTvmServiceConfigPtr& config)
{
    NTvmTool::TClientSettings settings(config->TvmToolSelfAlias);
    if (config->TvmToolPort) {
        settings.SetPort(config->TvmToolPort);
    }
    if (config->TvmToolAuthToken) {
        settings.SetAuthToken(*config->TvmToolAuthToken);
    }
    return settings;
}

NTvmApi::TClientSettings MakeTvmApiSettings(const TTvmServiceConfigPtr& config)
{
    NTvmApi::TClientSettings settings;
    settings.SetSelfTvmId(config->ClientSelfId);
    if (config->ClientDiskCacheDir) {
        settings.SetDiskCacheDir(*config->ClientDiskCacheDir);
    }
    if (config->TvmHost && config->TvmPort) {
        settings.SetTvmHostPort(*config->TvmHost, *config->TvmPort);
    }
    if (config->ClientEnableUserTicketChecking) {
        auto env = FromString<EBlackboxEnv>(config->ClientBlackboxEnv);
        settings.EnableUserTicketChecking(env);
    }
    if (config->ClientEnableServiceTicketFetching) {
        NTvmApi::TClientSettings::TDstMap dsts;
        for (const auto& [alias, dst] : config->ClientDstMap) {
            dsts[alias] = dst;
        }

        auto secret = config->ClientSelfSecret;
        if (!secret) {
            if (config->ClientSelfSecretEnv) {
                secret = GetEnv(*config->ClientSelfSecretEnv);
            } else if (config->ClientSelfSecretPath) {
                TFileInput input(*config->ClientSelfSecretPath);
                secret = input.ReadLine();
            }
        }
        if (secret) {
            settings.EnableServiceTicketsFetchOptions(*secret, std::move(dsts));
        }
    }
    if (config->ClientEnableServiceTicketChecking) {
        settings.EnableServiceTicketChecking();
    }
    return settings;
}

////////////////////////////////////////////////////////////////////////////////

class TTvmServiceBase
    : public virtual ITvmService
{
public:
    TTvmServiceBase(
        TTvmServiceConfigPtr config,
        TProfiler profiler)
        : Config_(std::move(config))
        , GetServiceTicketCounter_(profiler.WithPrefix("/get_service_ticket"))
        , ParseServiceTicketHitCounter_(profiler.WithTag("cache", "hit").WithPrefix("/parse_service_ticket"))
        , ParseServiceTicketMissCounter_(profiler.WithTag("cache", "miss").WithPrefix("/parse_service_ticket"))
        , ParseUserTicketCounter_(profiler.WithPrefix("/parse_user_ticket"))
        , ClientErrorCountCounter_(profiler.Counter("/client_error_count"))
    {
        if (Config_->EnableTicketParseCache) {
            TicketParseCache_ = std::make_unique<TTicketParseCache>(Config_->TicketCheckingCacheTimeout);
        }
    }

    TTvmId GetSelfTvmId() override
    {
        return Config_->ClientSelfId;
    }

    TString GetServiceTicket(const TString& serviceAlias) override
    {
        if (!Config_->ClientEnableServiceTicketFetching) {
            THROW_ERROR_EXCEPTION("Fetching service tickets is disabled");
        }

        YT_LOG_DEBUG("Retrieving TVM service ticket (ServiceAlias: %v)", serviceAlias);
        return DoGetServiceTicket(serviceAlias);
    }

    TString GetServiceTicket(TTvmId serviceId) override
    {
        if (!Config_->ClientEnableServiceTicketFetching) {
            THROW_ERROR_EXCEPTION("Fetching service tickets is disabled");
        }

        YT_LOG_DEBUG("Retrieving TVM service ticket (ServiceId: %v)", serviceId);
        return DoGetServiceTicket(serviceId);
    }

    TParsedTicket ParseUserTicket(const TString& ticket) override
    {
        if (!Config_->ClientEnableUserTicketChecking) {
            THROW_ERROR_EXCEPTION("Parsing user tickets disabled");
        }

        YT_LOG_DEBUG("Parsing user ticket (Ticket: %v)", NUtils::RemoveTicketSignature(ticket));
        TWallTimer timer;

        try {
            CheckClient();
            auto userTicket = GetClient().CheckUserTicket(ticket);
            if (!userTicket) {
                THROW_ERROR_EXCEPTION(TString(StatusToString(userTicket.GetStatus())));
            }

            TParsedTicket result;
            result.DefaultUid = userTicket.GetDefaultUid();
            for (const auto& scope : userTicket.GetScopes()) {
                result.Scopes.emplace(scope);
            }

            ParseUserTicketCounter_.OnSuccess(timer);
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_DEBUG(error);
            ParseUserTicketCounter_.OnFailure(timer);
            THROW_ERROR error;
        }
    }

    TParsedServiceTicket ParseServiceTicket(const TString& ticket) override
    {
        if (!Config_->ClientEnableServiceTicketChecking) {
            THROW_ERROR_EXCEPTION("Parsing service tickets is disabled");
        }

        YT_LOG_DEBUG("Parsing service ticket (Ticket: %v)", NUtils::RemoveTicketSignature(ticket));
        TWallTimer timer;

        if (TicketParseCache_) {
            if (auto resultOrError = TicketParseCache_->Get(ticket)) {
                if (resultOrError->IsOK()) {
                    ParseServiceTicketHitCounter_.OnSuccess(timer);
                    return std::move(resultOrError->Value());
                } else {
                    YT_LOG_DEBUG(resultOrError);
                    ParseServiceTicketHitCounter_.OnFailure(timer);
                    THROW_ERROR *resultOrError;
                }
            }
        }

        try {
            CheckClient();
            auto serviceTicket = GetClient().CheckServiceTicket(ticket);
            if (!serviceTicket) {
                THROW_ERROR_EXCEPTION(TString(StatusToString(serviceTicket.GetStatus())));
            }

            TParsedServiceTicket result;
            result.TvmId = serviceTicket.GetSrc();

            if (TicketParseCache_) {
                TicketParseCache_->Set(ticket, result);
            }

            ParseServiceTicketMissCounter_.OnSuccess(timer);
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);

            if (TicketParseCache_) {
                TicketParseCache_->Set(ticket, error);
            }

            YT_LOG_DEBUG(error);
            ParseServiceTicketMissCounter_.OnFailure(timer);
            THROW_ERROR error;
        }
    }

protected:
    const TTvmServiceConfigPtr Config_;

    virtual TTvmClient& GetClient() = 0;

private:
    struct TTicketCounter
    {
        TCounter Successful;
        TCounter Failed;
        TEventTimer SuccessTime;
        TEventTimer FailureTime;

        explicit TTicketCounter(const TProfiler& profiler)
            : Successful(profiler.WithTag("status", "ok").Counter("/success_count"))
            , Failed(profiler.WithTag("status", "fail").Counter("/count"))
            , SuccessTime(profiler.WithTag("status", "ok").Timer("/time"))
            , FailureTime(profiler.WithTag("status", "fail").Timer("/time"))
        { }

        void OnSuccess(const TWallTimer& timer)
        {
            Successful.Increment();
            SuccessTime.Record(timer.GetElapsedTime());
        }

        void OnFailure(const TWallTimer& timer)
        {
            Failed.Increment();
            FailureTime.Record(timer.GetElapsedTime());
        }
    };

    TTicketCounter GetServiceTicketCounter_;
    TTicketCounter ParseServiceTicketHitCounter_;
    TTicketCounter ParseServiceTicketMissCounter_;
    TTicketCounter ParseUserTicketCounter_;

    TCounter ClientErrorCountCounter_;

    using TTicketParseCache = TThreadLocalExpiringCache<TString, TErrorOr<TParsedServiceTicket>>;
    std::unique_ptr<TTicketParseCache> TicketParseCache_;

private:
    void CheckClient()
    {
        auto status = GetClient().GetStatus();
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

    TString DoGetServiceTicket(const auto& serviceId)
    {
        TWallTimer timer;

        try {
            CheckClient();
            // The client caches everything locally, no need for async.
            auto result = GetClient().GetServiceTicketFor(serviceId);
            GetServiceTicketCounter_.OnSuccess(timer);
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_WARNING(error);
            GetServiceTicketCounter_.OnFailure(timer);
            THROW_ERROR error;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTvmService
    : public TTvmServiceBase
{
public:
    TTvmService(
        TTvmServiceConfigPtr config,
        TProfiler profiler)
        : TTvmServiceBase(std::move(config), std::move(profiler))
    {
        if (Config_->ClientEnableUserTicketChecking ||
            Config_->ClientEnableServiceTicketChecking ||
            Config_->ClientEnableServiceTicketFetching)
        {
            MakeClient();
        }
    }

protected:
    TTvmClient& GetClient() override
    {
        YT_VERIFY(Client_);
        return *Client_;
    }

private:
    std::unique_ptr<TTvmClient> Client_;

    void MakeClient()
    {
        YT_LOG_INFO("Creating TVM client (UseTvmTool: %v)", Config_->UseTvmTool);

        auto logger = MakeIntrusive<TTvmLoggerAdapter>();

        if (Config_->UseTvmTool) {
            Client_ = std::make_unique<TTvmClient>(MakeTvmToolSettings(Config_), std::move(logger));
        } else {
            // If TVM is unreachable _and_ there are no cached keys, this will throw.
            // We'll just crash and restart.
            Client_ = std::make_unique<TTvmClient>(MakeTvmApiSettings(Config_), std::move(logger));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicTvmService
    : public TTvmServiceBase
    , public virtual IDynamicTvmService
{
public:
    TDynamicTvmService(
        TTvmServiceConfigPtr config,
        TProfiler profiler)
        : TTvmServiceBase(std::move(config), std::move(profiler))
    {
        if (Config_->ClientEnableUserTicketChecking ||
            Config_->ClientEnableServiceTicketChecking ||
            Config_->ClientEnableServiceTicketFetching)
        {
            MakeClient();
        }
    }

    void AddDestinationServiceIds(const std::vector<TTvmId>& serviceIds) override
    {
        NTvmApi::TDstSet dstSet(serviceIds.begin(), serviceIds.end());
        DynamicClient_->Add(std::move(dstSet));
    }

protected:
    TTvmClient& GetClient() override
    {
        return *Client_.get();
    }

private:
    ::TIntrusivePtr<NDynamicClient::TTvmClient> DynamicClient_;
    std::unique_ptr<TTvmClient> Client_;

    void MakeClient()
    {
        YT_LOG_INFO("Creating dynamic TVM client (UseTvmTool: %v)", Config_->UseTvmTool);

        if (Config_->UseTvmTool) {
            THROW_ERROR_EXCEPTION("TVM tool is not supported for dynamic client");
        }

        DynamicClient_ = NDynamicClient::TTvmClient::Create(MakeTvmApiSettings(Config_), MakeIntrusive<TTvmLoggerAdapter>());
        Client_ = std::make_unique<TTvmClient>(TAsyncUpdaterPtr(DynamicClient_));
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

class TMockTvmService
    : public IDynamicTvmService
{
public:
    explicit TMockTvmService(TTvmServiceConfigPtr config)
        : Config_(std::move(config))
    {
        DestinationAliases_ = Config_->ClientDstMap;
        for (const auto& [_, tvmId] : Config_->ClientDstMap) {
            DestinationIds_.insert(tvmId);
        }

        if (Config_->RequireMockSecret) {
            auto secret = Config_->ClientSelfSecret;
            if (!secret && Config_->ClientSelfSecretPath) {
                TFileInput input(*Config_->ClientSelfSecretPath);
                secret = Strip(input.ReadLine());
            }
            if (!secret) {
                THROW_ERROR_EXCEPTION("Secret for TVM testing stub is unspecified.");
            }
            if (secret != SecretPrefix + ToString(Config_->ClientSelfId)) {
                THROW_ERROR_EXCEPTION("Secret for TVM testing stub is invalid.");
            }
        }

        YT_LOG_DEBUG("Created TVM service testing stub");
    }

    TTvmId GetSelfTvmId() override
    {
        return Config_->ClientSelfId;
    }

    TString GetServiceTicket(const TString& serviceAlias) override
    {
        if (!Config_->ClientEnableServiceTicketFetching) {
            THROW_ERROR_EXCEPTION("Fetching service tickets is disabled");
        }

        YT_LOG_DEBUG("Retrieving TVM service ticket (ServiceAlias: %v)", serviceAlias);

        auto guard = ReaderGuard(Lock_);
        auto iter = DestinationAliases_.find(serviceAlias);
        if (iter == DestinationAliases_.end()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service alias %Qv is unknown", serviceAlias);
        }
        auto tvmId = iter->second;
        guard.Release();

        return MakeServiceTicket(Config_->ClientSelfId, tvmId);
    }

    TString GetServiceTicket(TTvmId serviceId) override
    {
        if (!Config_->ClientEnableServiceTicketFetching) {
            THROW_ERROR_EXCEPTION("Fetching service tickets is disabled");
        }

        YT_LOG_DEBUG("Retrieving TVM service ticket (ServiceId: %v)", serviceId);

        auto guard = ReaderGuard(Lock_);
        if (!DestinationIds_.contains(serviceId)) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service id %v is unknown", serviceId);
        }
        guard.Release();

        return MakeServiceTicket(Config_->ClientSelfId, serviceId);
    }

    TParsedTicket ParseUserTicket(const TString& /*ticket*/) override
    {
        THROW_ERROR_EXCEPTION("Parsing user tickets is not supported");
    }

    TParsedServiceTicket ParseServiceTicket(const TString& ticket) override
    {
        if (!Config_->ClientEnableServiceTicketChecking) {
            THROW_ERROR_EXCEPTION("Parsing service tickets is disabled");
        }

        YT_LOG_DEBUG("Parsing service ticket (Ticket: %v)", ticket);

        if (!ticket.StartsWith(TicketPrefix)) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Bad ticket header");
        }
        auto ticketBody = TStringBuf(ticket).substr(TicketPrefix.size());
        TStringBuf srcIdString;
        TStringBuf dstIdString;
        if (!ticketBody.TrySplit('-', srcIdString, dstIdString)) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Bad ticket body");
        }
        try {
            auto srcId = FromString<TTvmId>(srcIdString);
            auto dstId = FromString<TTvmId>(dstIdString);

            if (dstId != GetSelfTvmId()) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Destination TVM id mismatch");
            }

            return TParsedServiceTicket{.TvmId = srcId};
        } catch (const TFromStringException& exc) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Bad ticket body") << exc;
        }
    }

    void AddDestinationServiceIds(const std::vector<TTvmId>& serviceIds) override
    {
        auto guard = WriterGuard(Lock_);
        for (auto tvmId : serviceIds) {
            DestinationIds_.insert(tvmId);
        }
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TTvmServiceConfigPtr Config_;
    THashSet<ui32> DestinationIds_;
    THashMap<TString, ui32> DestinationAliases_;

    static const TString TicketPrefix;
    static const TString SecretPrefix;

    static TString MakeServiceTicket(TTvmId src, TTvmId dst)
    {
        return Format("%v%v-%v", TicketPrefix, src, dst);
    }
};

const TString TMockTvmService::TicketPrefix = "TestTicket-";
const TString TMockTvmService::SecretPrefix = "TestSecret-";

} // namespace

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateTvmService(
    TTvmServiceConfigPtr config,
    TProfiler profiler)
{
    if (config->EnableMock) {
        return New<TMockTvmService>(std::move(config));
    }

    return New<TTvmService>(
        std::move(config),
        std::move(profiler));
}

IDynamicTvmServicePtr CreateDynamicTvmService(
    TTvmServiceConfigPtr config,
    TProfiler profiler)
{
    if (config->EnableMock) {
        return New<TMockTvmService>(std::move(config));
    }

    return New<TDynamicTvmService>(
        std::move(config),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf RemoveTicketSignature(TStringBuf ticketBody)
{
    return NTvmAuth::NUtils::RemoveTicketSignature(ticketBody);
}

////////////////////////////////////////////////////////////////////////////////

bool IsDummyTvmServiceImplementation()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
