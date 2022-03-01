#include "hedging_executor.h"

#include "logger.h"

#include <yt/yt/core/logging/log.h>


namespace NYT::NClient::NHedging::NRpc {

THedgingClientOptions::TClientOptions::TClientOptions(
    NYT::NApi::IClientPtr client,
    TString clusterName,
    TDuration initialPenalty,
    TCounterPtr counter
)
    : Client(std::move(client))
    , ClusterName(std::move(clusterName))
    , InitialPenalty(initialPenalty)
    , Counter(std::move(counter))
{
}

THedgingClientOptions::TClientOptions::TClientOptions(
    NYT::NApi::IClientPtr client,
    TDuration initialPenalty,
    TCounterPtr counter
)
    : TClientOptions(client, "default", initialPenalty, counter)
{
}

THedgingExecutor::THedgingExecutor(const THedgingClientOptions& options, const IPenaltyProviderPtr& penaltyProvider)
    : BanPenalty_(NYT::NProfiling::DurationToCpuDuration(options.BanPenalty))
    , BanDuration_(NYT::NProfiling::DurationToCpuDuration(options.BanDuration))
    , PenaltyProvider_(penaltyProvider)
{
    Y_ENSURE(!options.Clients.empty(), "Clients should not be empty!");
    for (const auto& clientOptions : options.Clients) {
        Y_ENSURE(clientOptions.Client, "Client pointer should be valid!");
        Clients_.emplace_back(
            clientOptions.Client,
            NYT::NProfiling::DurationToCpuDuration(clientOptions.InitialPenalty),
            clientOptions.Counter ? clientOptions.Counter : NYT::New<TCounter>(clientOptions.Client->GetConnection()->GetClusterId()),
            clientOptions.ClusterName);
    }
}

NYT::NApi::IConnectionPtr THedgingExecutor::GetConnection() {
    return Clients_[0].Client->GetConnection();
}

void THedgingExecutor::OnFinishRequest(
    size_t clientIndex,
    TDuration effectivePenalty,
    NYT::NProfiling::TCpuDuration adaptivePenalty,
    NYT::NProfiling::TCpuDuration externalPenalty,
    NYT::NProfiling::TCpuInstant start,
    const NYT::TError& error
) {
    auto& clientInfo = Clients_[clientIndex];
    if (error.IsOK()) {
        if (adaptivePenalty) {
            TGuard guard(SpinLock_);
            clientInfo.BanUntil = Max<NYT::NProfiling::TCpuInstant>();
            clientInfo.AdaptivePenalty = 0;
        }
        clientInfo.Counter->SuccessRequestCount.Increment();
        clientInfo.Counter->RequestDuration.Record(NYT::NProfiling::CpuDurationToDuration(NYT::NProfiling::GetCpuInstant() - start));
    } else if (effectivePenalty && (error.GetCode() == NYT::EErrorCode::Canceled || error.GetCode() == NYT::EErrorCode::FutureCombinerShortcut)) {
        clientInfo.Counter->CancelRequestCount.Increment();
    } else {
        with_lock (SpinLock_) {
            clientInfo.BanUntil = NYT::NProfiling::GetCpuInstant() + BanDuration_;
            clientInfo.AdaptivePenalty += BanPenalty_;
        }
        clientInfo.Counter->ErrorRequestCount.Increment();
        YT_LOG_WARNING("client#%v failed with error %v", clientIndex, error);
    }
    clientInfo.Counter->EffectivePenalty.Update(effectivePenalty);
    clientInfo.Counter->ExternalPenalty.Update(NYT::NProfiling::CpuDurationToDuration(externalPenalty));
}

THedgingExecutor::TEntry::TEntry(NYT::NApi::IClientPtr client, NYT::NProfiling::TCpuDuration initialPenalty, TCounterPtr counter, TString clusterName)
    : Client(std::move(client))
    , ClusterName(std::move(clusterName))
    , AdaptivePenalty(0)
    , InitialPenalty(initialPenalty)
    , ExternalPenalty(0)
    , BanUntil(Max<NYT::NProfiling::TCpuInstant>())
    , Counter(std::move(counter))
{
}

} // namespace NYT::NClient::NHedging::NRpc
