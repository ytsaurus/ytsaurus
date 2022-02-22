#include "penalty_provider.h"

#include "counter.h"
#include "logger.h"
#include "public.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/misc/error.h>

#include <util/generic/hash.h>
#include <util/generic/xrange.h>

namespace {

using namespace NYT::NHedgingClient::NRpc;

class TDummyLagProvider: public IPenaltyProvider {
public:
    NYT::NProfiling::TCpuDuration Get(const TString&) override {
        return 0;
    }
};

class TLagPenaltyProvider: public IPenaltyProvider {
public:
    TLagPenaltyProvider(const TReplicaionLagPenaltyProviderConfig& config, NYT::NApi::IClientPtr masterClient)
        : TablePath_(config.GetTablePath())
        , MaxTabletLag_(TDuration::Seconds(config.GetMaxTabletLag()))
        , LagPenalty_(NYT::NProfiling::DurationToCpuDuration(TDuration::MilliSeconds(config.GetLagPenalty())))
        , MaxTabletsWithLagFraction_(config.GetMaxTabletsWithLagFraction())
        , MasterClient_(masterClient)
        , ClearPenaltiesOnErrors_(config.GetClearPenaltiesOnErrors())
        , Counters_(NYT::New<TLagPenaltyProviderCounters>(TablePath_,
            TVector<TString>{config.GetReplicaClusters().begin(), config.GetReplicaClusters().end()}))
        , Executor_(NYT::New<NYT::NConcurrency::TPeriodicExecutor>(
            NYT::NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TLagPenaltyProvider::UpdateCurrentLagPenalty, NYT::MakeWeak(this)),
            TDuration::Seconds(config.GetCheckPeriod())))
    {
        Y_ENSURE(Executor_);
        Y_ENSURE(MasterClient_);

        for (const auto& cluster : config.GetReplicaClusters()) {
            auto [_, inserted] = ReplicaClusters_.try_emplace(cluster);
            Y_ENSURE(inserted, "Replica cluster " << cluster << " is listed twice");
        }

        GetNodeOptions_.Timeout = TDuration::Seconds(5);
        GetNodeOptions_.ReadFrom = NYT::NApi::EMasterChannelKind::Cache;

        Executor_->Start();
    }

    /**
     * Checks that all ReplicaIds in ReplicaClusters_ have been set
     */
    NYT::TError CheckAllReplicaIdsPresent() const {
        for (const auto& [cluster, info] : ReplicaClusters_) {
            if (!info.ReplicaId) {
                return NYT::TError{"ReplicaId was not found for %v", cluster};
            }
        }
        return {};
    }

    /**
     * Fills ReplicaIds in ReplicaClusters_
     */
    void UpdateReplicaIds() {
        auto replicasNode = NYT::NYTree::ConvertToNode(NYT::NConcurrency::WaitFor(MasterClient_->GetNode(TablePath_ + "/@replicas", GetNodeOptions_)).ValueOrThrow())->AsMap();

        for (const auto& row : replicasNode->GetChildren()) {
            TString cluster = row.second->AsMap()->GetChildOrThrow("cluster_name")->AsString()->GetValue();
            if (auto* info = ReplicaClusters_.FindPtr(cluster)) {
                info->ReplicaId = NYT::NTabletClient::TTableReplicaId::FromString(row.first);
                YT_LOG_INFO("Found ReplicaId %v for table %v in cluster %v", info->ReplicaId, TablePath_, cluster);
            };
        }
        CheckAllReplicaIdsPresent().ThrowOnError();
    }

    ui64 GetTotalNumberOfTablets() {
        return NYT::NYTree::ConvertTo<ui64>(NYT::NConcurrency::WaitFor(MasterClient_->GetNode(TablePath_ + "/@tablet_count", GetNodeOptions_)).ValueOrThrow());
    }

    /**
     * Returns a map: ReplicaId -> # of tablets
     */
    THashMap<NYT::NTabletClient::TTableReplicaId, ui64> CalculateNumbersOfTabletsWithLag(const ui64 tabletsCount) {
        auto tabletsRange = xrange(tabletsCount);
        auto tabletsInfo = NYT::NConcurrency::WaitFor(MasterClient_->GetTabletInfos(TablePath_, {tabletsRange.begin(), tabletsRange.end()})).ValueOrThrow();

        const auto now = TInstant::Now();
        THashMap<NYT::NTabletClient::TTableReplicaId, ui64> tabletsWithLag;

        for (const auto& tabletInfo : tabletsInfo) {
            if (!tabletInfo.TableReplicaInfos) {
                continue;
            }

            for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                auto lastReplicationTimestamp = TInstant::Seconds(NYT::NTransactionClient::UnixTimeFromTimestamp(replicaInfo.LastReplicationTimestamp));
                if (now - lastReplicationTimestamp > MaxTabletLag_) {
                    ++tabletsWithLag[replicaInfo.ReplicaId];
                }
            }
        }

        return tabletsWithLag;
    }

    NYT::NProfiling::TCpuDuration CalculateLagPenalty(const ui64 tabletsCount, const ui64 tabletsWithLag) {
        return tabletsWithLag >= tabletsCount * MaxTabletsWithLagFraction_ ? LagPenalty_ : 0;
    }

    void UpdateCurrentLagPenalty() {
        try {
            YT_LOG_INFO("Start penalty updater check for: %v", TablePath_);

            if (!CheckAllReplicaIdsPresent().IsOK()) {
                UpdateReplicaIds();
            }

            auto tabletsCount = GetTotalNumberOfTablets();
            auto tabletsWithLag = CalculateNumbersOfTabletsWithLag(tabletsCount);

            Counters_->TotalTabletsCount.Update(tabletsCount);

            for (auto& [cluster, info] : ReplicaClusters_) {
                Y_ASSERT(info.ReplicaId);
                auto curTabletsWithLag = tabletsWithLag.Value(info.ReplicaId, 0);
                NYT::NProfiling::TCpuDuration newLagPenalty = CalculateLagPenalty(tabletsCount, curTabletsWithLag);
                info.CurrentLagPenalty.store(newLagPenalty, std::memory_order_relaxed);

                Counters_->LagTabletsCount.at(cluster).Update(curTabletsWithLag);
                YT_LOG_INFO(
                    "Finish penalty updater check (%v: %v/%v tablets lagging => penalty %v ms) for: %v",
                    cluster, curTabletsWithLag, tabletsCount,
                    NYT::NProfiling::CpuDurationToDuration(newLagPenalty).MilliSeconds(),
                    TablePath_
                );
            }

            Counters_->SuccessRequestCount.Increment();
        } catch (const std::exception& err) {
            Counters_->ErrorRequestCount.Increment();

            YT_LOG_ERROR("Lag penalty updater for %v failed: %v", TablePath_, err.what());

            if (ClearPenaltiesOnErrors_) {
                for (auto& [cluster, info] : ReplicaClusters_) {
                    info.CurrentLagPenalty.store(0, std::memory_order_relaxed);
                    YT_LOG_INFO("Clearing penalty for cluster %v and table %v", cluster, TablePath_);
                }
            }
        }
    }

    NYT::NProfiling::TCpuDuration Get(const TString& cluster) override {
        if (const TReplicaInfo* info = ReplicaClusters_.FindPtr(cluster)) {
            return info->CurrentLagPenalty.load(std::memory_order_relaxed);
        }
        return 0;
    }

    ~TLagPenaltyProvider() {
        Executor_->Stop();
    }

private:
    struct TReplicaInfo {
        NYT::NTabletClient::TTableReplicaId ReplicaId = {};
        std::atomic<NYT::NProfiling::TCpuDuration> CurrentLagPenalty = 0;
    };

    const TString TablePath_;
    THashMap<TString, TReplicaInfo> ReplicaClusters_;
    const TDuration MaxTabletLag_;
    const NYT::NProfiling::TCpuDuration LagPenalty_;
    const float MaxTabletsWithLagFraction_;
    NYT::NApi::IClientPtr MasterClient_;
    const bool ClearPenaltiesOnErrors_;
    TLagPenaltyProviderCountersPtr Counters_;
    NYT::NApi::TGetNodeOptions GetNodeOptions_;
    NYT::NConcurrency::TPeriodicExecutorPtr Executor_;
};

} // namespace

namespace NYT::NHedgingClient::NRpc {

IPenaltyProviderPtr CreateDummyPenaltyProvider() {
    return NYT::New<TDummyLagProvider>();
}

IPenaltyProviderPtr CreateReplicaionLagPenaltyProvider(const TReplicaionLagPenaltyProviderConfig& config, NYT::NApi::IClientPtr masterClient) {
    return NYT::New<TLagPenaltyProvider>(config, masterClient);
}

} // namespace NYT::NHedgingClient::NRpc
