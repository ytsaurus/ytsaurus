#include "tvm_bridge.h"

#include "private.h"
#include "tvm_bridge_service_proxy.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/backoff_strategy_api.h>
#include <yt/yt/core/misc/backoff_strategy_config.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NRpc;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TTvmBridge
    : public ITvmBridge
{
public:
    TTvmBridge(
        IInvokerPtr invoker,
        IChannelPtr channel,
        TTvmBridgeConfigPtr config)
        : Invoker_(std::move(invoker))
        , Config_(std::move(config))
        , Logger(TvmBridgeLogger)
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TTvmBridge::RefreshTickets, MakeWeak(this)),
            Config_->RefreshPeriod))
        , Proxy_(std::move(channel))
    {
        Proxy_.SetDefaultTimeout(Config_->RpcTimeout);
        PeriodicExecutor_->Start();
    }

    TFuture<void> EnsureDestinationServiceIds(const std::vector<TTvmId>& serviceIds) override
    {
        return BIND(
            &TTvmBridge::DoEnsureDestinationServiceIds,
            MakeStrong(this),
            serviceIds)
                .AsyncVia(Invoker_)
                .Run();
    }

    void AddDestinationServiceIds(const std::vector<TTvmId>& serviceIds) override
    {
        auto guard = WriterGuard(Lock_);
        for (auto serviceId : serviceIds) {
            if (!ServiceIdToTicket_.contains(serviceId)) {
                ServiceIdToTicket_.emplace(
                    serviceId,
                    TError(NRpc::EErrorCode::Unavailable, "Ticket is not fetched"));
            }
        }
        guard.Release();

        PeriodicExecutor_->ScheduleOutOfBand();
    }

    TString GetServiceTicket(TTvmId serviceId) override
    {
        auto guard = ReaderGuard(Lock_);

        auto iter = ServiceIdToTicket_.find(serviceId);
        if (iter == ServiceIdToTicket_.end()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Ticket is not fetched");
        }

        return iter->second.ValueOrThrow();
    }

    TTvmId GetSelfTvmId() override
    {
        return Config_->SelfTvmId;
    }

    TString GetServiceTicket(const TString& /*serviceAlias*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TParsedTicket ParseUserTicket(const TString& /*ticket*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TParsedServiceTicket ParseServiceTicket(const TString& /*ticket*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IInvokerPtr Invoker_;
    const TTvmBridgeConfigPtr Config_;

    const TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<TTvmId, TErrorOr<TString>> ServiceIdToTicket_;

    TPeriodicExecutorPtr PeriodicExecutor_;

    TTvmBridgeServiceProxy Proxy_;

    static constexpr int SampleServiceIdCount = 10;

    void DoEnsureDestinationServiceIds(std::vector<TTvmId> serviceIds)
    {
        TBackoffStrategy backoff(Config_->EnsureTicketsBackoff);

        auto filterFetchedTickets = [&] {
            auto guard = ReaderGuard(Lock_);

            auto endIter = std::remove_if(
                serviceIds.begin(),
                serviceIds.end(),
                [this] (auto serviceId) {
                    auto iter = ServiceIdToTicket_.find(serviceId);
                    return iter != ServiceIdToTicket_.end() && iter->second.IsOK();
                });

            serviceIds.erase(endIter, serviceIds.end());
        };

        filterFetchedTickets();
        if (serviceIds.empty()) {
            YT_LOG_DEBUG("No new tickets to fetch");
            return;
        }

        while (true) {
            YT_LOG_INFO("Fetching missing service tickets (ServiceIds: %v, Count: %v, AttemptIndex: %v/%v)",
                MakeShrunkFormattableView(serviceIds, TDefaultFormatter(), SampleServiceIdCount),
                serviceIds.size(),
                backoff.GetInvocationIndex(),
                backoff.GetInvocationCount());

            auto req = Proxy_.FetchTickets();
            req->set_source(Config_->SelfTvmId);
            ToProto(req->mutable_destinations(), serviceIds);

            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            UpdateServiceTickets(*rsp);

            filterFetchedTickets();
            if (serviceIds.empty()) {
                YT_LOG_INFO("All tickets fetched successfully");
                return;
            }
            if (!backoff.Next()) {
                break;
            }

            auto backoffDuration = backoff.GetBackoff();
            YT_LOG_DEBUG("There are unfetched tickets, backing off (Duration: %v)", backoffDuration);
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }

        YT_LOG_ERROR("Failed to fetch service tickets (ServiceIds: %v, Count: %v)",
            MakeShrunkFormattableView(serviceIds, TDefaultFormatter(), SampleServiceIdCount),
            serviceIds.size());

        THROW_ERROR_EXCEPTION("Failed to fetch service tickets");
    }

    void UpdateServiceTickets(const NProto::TRspFetchTickets& rsp)
    {
        auto guard = WriterGuard(Lock_);

        for (auto result : rsp.results()) {
            auto destination = result.destination();

            if (result.has_ticket()) {
                auto iter = ServiceIdToTicket_.find(destination);
                if (iter == ServiceIdToTicket_.end() ||
                    !iter->second.IsOK() ||
                    iter->second.Value() != result.ticket())
                {
                    YT_LOG_DEBUG("Service ticket updated (Destination: %v)", destination);
                }

                ServiceIdToTicket_[destination] = result.ticket();
            } else {
                YT_VERIFY(result.has_error());

                auto error = FromProto<TError>(result.error());
                auto iter = ServiceIdToTicket_.find(destination);
                if (iter == ServiceIdToTicket_.end() || !iter->second.IsOK()) {
                    ServiceIdToTicket_[destination] = error;
                }

                YT_LOG_WARNING(error, "Failed to fetch service ticket");
            }
        }
    }

    void RefreshTickets()
    {
        YT_LOG_DEBUG("Refreshing service tickets");

        auto req = Proxy_.FetchTickets();
        {
            auto guard = ReaderGuard(Lock_);
            req->set_source(Config_->SelfTvmId);
            ToProto(req->mutable_destinations(), GetKeys(ServiceIdToTicket_));
        }

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to refresh service tickets");
            return;
        }

        UpdateServiceTickets(*rspOrError.Value());

        YT_LOG_DEBUG("Service tickets refreshed");
    }
};

////////////////////////////////////////////////////////////////////////////////

ITvmBridgePtr CreateTvmBridge(
    IInvokerPtr invoker,
    IChannelPtr channel,
    TTvmBridgeConfigPtr config)
{
    return New<TTvmBridge>(
        std::move(invoker),
        std::move(channel),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
