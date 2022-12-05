#include "tvm_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/library/auth/tvm.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/logger.h>
#include <library/cpp/tvmauth/client/misc/api/dynamic_dst/tvm_client.h>
#include <library/cpp/tvmauth/utils.h>

#include <util/system/mutex.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NHttp;
using namespace NYPath;
using namespace NConcurrency;
using namespace NTvmAuth;
using namespace NProfiling;

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

NTvmTool::TClientSettings MakeTvmToolSettings(const TTvmServiceConfigPtr& config)
{
    NTvmTool::TClientSettings settings(config->TvmToolSelfAlias);
    settings.SetPort(config->TvmToolPort);
    settings.SetAuthToken(config->TvmToolAuthToken);
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
        settings.EnableServiceTicketsFetchOptions(config->ClientSelfSecret, std::move(dsts));
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
        , GetServiceTicketCountCounter_(profiler.Counter("/get_service_ticket_count"))
        , SuccessfulGetServiceTicketCountCounter_(profiler.Counter("/successful_get_service_ticket_count"))
        , FailedGetServiceTicketCountCounter_(profiler.Counter("/failed_get_service_ticket_count"))
        , GetServiceTicketTime_(profiler.Timer("/get_service_ticket_time"))
        , ParseUserTicketCountCounter_(profiler.Counter("/parse_user_ticket_count"))
        , SuccessfulParseUserTicketCountCounter_(profiler.Counter("/successful_parse_user_ticket_count"))
        , FailedParseUserTicketCountCounter_(profiler.Counter("/failed_parse_user_ticket_count"))
        , ParseUserTicketTime_(profiler.Timer("/parse_user_ticket_time"))
        , ClientErrorCountCounter_(profiler.Counter("/client_error_count"))
        , ParseServiceTicketCountCounter_(profiler.Counter("/parse_service_ticket_count"))
        , SuccessfulParseServiceTicketCountCounter_(profiler.Counter("/successful_parse_service_ticket_count"))
        , FailedParseServiceTicketCountCounter_(profiler.Counter("/failed_parse_service_ticket_count"))
        , ParseServiceTicketTime_(profiler.Timer("/parse_service_ticket_time"))
    { }

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
        ParseUserTicketCountCounter_.Increment();
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

            SuccessfulParseUserTicketCountCounter_.Increment();
            ParseUserTicketTime_.Record(timer.GetElapsedTime());
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_WARNING(error);
            FailedParseUserTicketCountCounter_.Increment();
            THROW_ERROR error;
        }
    }

    TParsedServiceTicket ParseServiceTicket(const TString& ticket) override
    {
        if (!Config_->ClientEnableServiceTicketChecking) {
            THROW_ERROR_EXCEPTION("Parsing service tickets is disabled");
        }

        YT_LOG_DEBUG("Parsing service ticket (Ticket: %v)", NUtils::RemoveTicketSignature(ticket));
        ParseServiceTicketCountCounter_.Increment();
        TWallTimer timer;

        try {
            CheckClient();
            auto serviceTicket = GetClient().CheckServiceTicket(ticket);
            if (!serviceTicket) {
                THROW_ERROR_EXCEPTION(TString(StatusToString(serviceTicket.GetStatus())));
            }

            TParsedServiceTicket result;
            result.TvmId = serviceTicket.GetSrc();

            SuccessfulParseServiceTicketCountCounter_.Increment();
            ParseServiceTicketTime_.Record(timer.GetElapsedTime());
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_WARNING(error);
            FailedParseServiceTicketCountCounter_.Increment();
            THROW_ERROR error;
        }
    }

protected:
    const TTvmServiceConfigPtr Config_;

    virtual TTvmClient& GetClient() = 0;

private:
    TCounter GetServiceTicketCountCounter_;
    TCounter SuccessfulGetServiceTicketCountCounter_;
    TCounter FailedGetServiceTicketCountCounter_;
    TEventTimer GetServiceTicketTime_;

    TCounter ParseUserTicketCountCounter_;
    TCounter SuccessfulParseUserTicketCountCounter_;
    TCounter FailedParseUserTicketCountCounter_;
    TEventTimer ParseUserTicketTime_;

    TCounter ClientErrorCountCounter_;

    TCounter ParseServiceTicketCountCounter_;
    TCounter SuccessfulParseServiceTicketCountCounter_;
    TCounter FailedParseServiceTicketCountCounter_;
    TEventTimer ParseServiceTicketTime_;

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
        GetServiceTicketCountCounter_.Increment();

        try {
            CheckClient();
            // The client caches everything locally, no need for async.
            auto result = GetClient().GetServiceTicketFor(serviceId);
            SuccessfulGetServiceTicketCountCounter_.Increment();
            GetServiceTicketTime_.Record(timer.GetElapsedTime());
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "TVM call failed") << TError(ex);
            YT_LOG_WARNING(error);
            FailedGetServiceTicketCountCounter_.Increment();
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
        if (Config_->ClientEnableUserTicketChecking || Config_->ClientEnableServiceTicketFetching) {
            MakeClient();
        }
    }

protected:
    TTvmClient& GetClient() override
    {
        return *Client_.get();
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
        if (Config_->ClientEnableUserTicketChecking || Config_->ClientEnableServiceTicketFetching) {
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

    static TString MakeServiceTicket(TTvmId src, TTvmId dst)
    {
        return Format("%v%v-%v", TicketPrefix, src, dst);
    }
};

const TString TMockTvmService::TicketPrefix = "TestTicket-";

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

} // namespace NYT::NAuth
