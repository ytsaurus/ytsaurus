#pragma once

#include "counter.h"
#include "penalty_provider.h"
#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

// @brief options for hedging client.
struct THedgingClientOptions
{
    struct TClientOptions
    {
        TClientOptions(NYT::NApi::IClientPtr client,
                      TString clusterName,
                      TDuration initialPenalty,
                      TCounterPtr counter = {});

        TClientOptions(NYT::NApi::IClientPtr client,
                       TDuration initialPenalty,
                       TCounterPtr counter = {});

        NYT::NApi::IClientPtr Client;
        TString ClusterName;
        TDuration InitialPenalty;
        TCounterPtr Counter;
    };

    TVector<TClientOptions> Clients;
    TDuration BanPenalty;
    TDuration BanDuration;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THedgingExecutor);

class THedgingExecutor final
{
public:
    THedgingExecutor(const THedgingClientOptions& options, const IPenaltyProviderPtr& penaltyProvider);

    NYT::NApi::IConnectionPtr GetConnection();

    template <typename T>
    NYT::TFuture<T> DoWithHedging(NYT::TCallback<NYT::TFuture<T>(NYT::NApi::IClientPtr)> callback)
    {
        NYT::NProfiling::TCpuInstant now = NYT::NProfiling::GetCpuInstant();
        auto clients = [=]() -> auto {
            TGuard guard(SpinLock_);
            for (auto& client : Clients_) {
                if (client.BanUntil < now) {
                    client.AdaptivePenalty = 0;
                }
            }

            return Clients_;
        }();

        NYT::NProfiling::TCpuDuration minInitialPenalty = Max<i64>();
        for (auto& client : clients) {
            client.ExternalPenalty = PenaltyProvider_->Get(client.ClusterName);
            NYT::NProfiling::TCpuDuration currentInitialPenalty = client.InitialPenalty + client.AdaptivePenalty + client.ExternalPenalty;
            minInitialPenalty = Min(minInitialPenalty, currentInitialPenalty);
        }

        TVector<NYT::TFuture<T>> futures(Reserve(clients.size()));
        for (auto [i, client] : Enumerate(clients)) {
            TDuration effectivePenalty = NYT::NProfiling::CpuDurationToDuration(client.InitialPenalty + client.AdaptivePenalty + client.ExternalPenalty - minInitialPenalty);
            if (effectivePenalty) {
                auto delayedFuture = NYT::NConcurrency::TDelayedExecutor::MakeDelayed(effectivePenalty, NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker());
                futures.push_back(delayedFuture.Apply(BIND(callback, client.Client)));
            } else {
                futures.push_back(callback(client.Client));
            }
            futures.back().Subscribe(BIND(&THedgingExecutor::OnFinishRequest, NYT::MakeWeak(this), i, effectivePenalty, client.AdaptivePenalty, client.ExternalPenalty, now));
        }

        return NYT::AnySucceeded(std::move(futures));
    }

private:
    void OnFinishRequest(
        size_t clientIndex,
        TDuration effectivePenalty,
        NYT::NProfiling::TCpuDuration adaptivePenalty,
        NYT::NProfiling::TCpuDuration externalPenalty,
        NYT::NProfiling::TCpuInstant start,
        const NYT::TError& r);

    struct TEntry
    {
        TEntry(NYT::NApi::IClientPtr client,
               NYT::NProfiling::TCpuDuration initialPenalty,
               TCounterPtr counter,
               TString clusterName);

        NYT::NApi::IClientPtr Client;
        TString ClusterName;
        NYT::NProfiling::TCpuDuration AdaptivePenalty;
        NYT::NProfiling::TCpuDuration InitialPenalty;
        NYT::NProfiling::TCpuDuration ExternalPenalty;
        NYT::NProfiling::TCpuInstant BanUntil;
        TCounterPtr Counter;
    };

    TVector<TEntry> Clients_;
    NYT::NProfiling::TCpuDuration BanPenalty_;
    NYT::NProfiling::TCpuDuration BanDuration_;
    IPenaltyProviderPtr PenaltyProvider_;
    TSpinLock SpinLock_;
};

DEFINE_REFCOUNTED_TYPE(THedgingExecutor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
