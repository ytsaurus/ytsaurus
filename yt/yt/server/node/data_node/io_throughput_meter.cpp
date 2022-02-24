#include "io_throughput_meter.h"

#include "chunk_store.h"
#include "blob_chunk.h"
#include "location.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/io/gentle_loader.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

class TRandomFileProvider
    : public NIO::IRandomFileProvider
{
public:
    TRandomFileProvider(TChunkStorePtr chunkStore, TChunkLocationUuid locationId, NLogging::TLogger logger)
        : Logger(std::move(logger))
    {
        NProfiling::TWallTimer timer;

        for (const auto& chunk : chunkStore->GetChunks()) {
            auto location = chunk->GetLocation();
            if (chunk->IsActive() || chunk->IsRemoveScheduled() || location->GetUuid() != locationId) {
                continue;
            }
            try {
                TFileInfo info{
                    .Path = chunk->GetFileName(),
                    .DiskSpace = NFS::GetFileStatistics(chunk->GetFileName()).Size
                };
                Chunks_.push_back(std::move(info));
            } catch (const std::exception&) {
            }
        }

        YT_LOG_DEBUG("Loaded chunks (Location: %v, Count: %v, ElapsedTime: %v)",
            Chunks_.size(),
            locationId,
            timer.GetElapsedTime());
    }

    std::optional<TFileInfo> GetRandomExistingFile() override
    {
        if (Chunks_.empty()) {
            return {};
        }
        return Chunks_[RandomNumber(Chunks_.size())];
    }

private:
    const NLogging::TLogger Logger;
    std::vector<TFileInfo> Chunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TLocationLoadTester
    : public TRefCounted
{
public:
    TLocationLoadTester(
        TChunkStorePtr chunkStore,
        TStoreLocationPtr location,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : ChunkStore_(std::move(chunkStore))
        , Location_(std::move(location))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
        , DiskReadCapacity_(Location_->GetProfiler().Gauge("/disk_read_capacity"))
        , DiskWriteCapacity_(Location_->GetProfiler().Gauge("/disk_write_capacity"))
    { }

    void Run(
        TIOThroughputMeterConfigPtr config,
        TMediumThroughputMeterConfigPtr mediumConfig)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Starting load test (Location: %v)",
            Location_->GetPath());

        auto randomFileProvider = New<TRandomFileProvider>(ChunkStore_, Location_->GetUuid(), Logger);
        auto loader = CreateGentleLoader(
            mediumConfig,
            Location_->GetPath(),
            Location_->GetIOEngineModel(),
            randomFileProvider,
            Invoker_,
            Logger);

        auto now = TInstant::Now();
        Session_ = TSession{
            .Loader = loader,
            .Timestamp = now,
            .LastCongested = now,
            .Config = config,
            .MediumConfig = mediumConfig,
        };

        loader->SubscribeCongested(
            BIND(&TLocationLoadTester::SessionCongested, MakeWeak(this), Session_->Timestamp));
        loader->Start();
    }

    bool Running() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return !!Session_;
    }

    void Stop()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        std::optional<TSession> session;
        std::swap(session, Session_);

        if (ScheduledAt_ || session) {
            YT_LOG_DEBUG("Stopping load test (Location: %v)",
                Location_->GetPath());
        }

        if (session) {
            session->Loader->Stop();
        }
        ScheduledAt_.reset();
    }

    TString GetMediumName() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return Location_->GetMediumName();
    }

    void SetScheduledTime(TInstant scheduledTime)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!ScheduledAt_);

        YT_LOG_DEBUG("Scheduled load test (Location: %v, ScheduledTime: %v)",
            Location_->GetPath(),
            scheduledTime);

        ScheduledAt_ = scheduledTime;
    }

    std::optional<TInstant> GetScheduledTime()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        return ScheduledAt_;
    }

    TDuration GetRunningTime()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Session_) {
            return TInstant::Now() - Session_->Timestamp;
        }
        return {};
    }

    TStoreLocation::TIOStatistics GetMeasured()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        return LastMeasuredThroughtput_;
    }

    TString GetRootPath() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return Location_->GetPath();
    }

private:
    struct TSession
    {
        NIO::IGentleLoaderPtr Loader;
        TInstant Timestamp;
        TInstant LastCongested;

        TIOThroughputMeterConfigPtr Config;
        TMediumThroughputMeterConfigPtr MediumConfig;

        TStoreLocation::TIOStatistics BestRoundResult;
        TDuration BestRoundDuration;
        int RoundsCount = 0;
    };

    const TChunkStorePtr ChunkStore_;
    const TStoreLocationPtr Location_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    std::optional<TSession> Session_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TStoreLocation::TIOStatistics LastMeasuredThroughtput_;
    TInstant LastFinishedTime_ = TInstant::Now();

    NProfiling::TGauge DiskReadCapacity_;
    NProfiling::TGauge DiskWriteCapacity_;

    std::optional<TInstant> ScheduledAt_;

    void SessionCongested(TInstant sessionTimestamp, i64 /*congestionWindow*/)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!Session_ || Session_->Timestamp != sessionTimestamp) {
            // Stale event.
            return;
        }

        const auto& config = Session_->Config;

        // Testing round is defined as a time between two congestions.
        // We treat each round as independent testing attempt and favor result
        // with longer duration.
        // Due nature of congestion control algorithm (additive increase) and
        // modern flash drives (write saturation / steady state after some time)
        // longer test duration corelate with more accurate result.
        auto now = TInstant::Now();
        auto roundDuration = now - Session_->LastCongested;
        auto roundResult = Location_->GetIOStatistics();

        // Use filesystem level statistics if disk stats are not available.
        // This is mostly for test environment.
        if (roundResult.DiskReadRate == 0 && roundResult.DiskWriteRate == 0) {
            roundResult.DiskReadRate = roundResult.FilesystemReadRate;
            roundResult.DiskWriteRate = roundResult.FilesystemWriteRate;
        }

        if (roundDuration > Session_->BestRoundDuration) {
            Session_->BestRoundDuration = roundDuration;
            Session_->BestRoundResult = roundResult;
        }

        Session_->LastCongested = now;
        ++Session_->RoundsCount;

        auto overallDuration = now - Session_->Timestamp;

        if (Session_->RoundsCount > config->MaxCongestionsPerTest || overallDuration > config->TestingTimeSoftLimit) {
            YT_LOG_WARNING("Setting test results (Location: %v, OverallDuration: %v, RoundsCount: %v, "
                "DiskReadRate: %v, DiskWriteRate: %v, BestRoundDuration: %v)",
                Location_->GetPath(),
                overallDuration,
                Session_->RoundsCount,
                Session_->BestRoundResult.DiskReadRate,
                Session_->BestRoundResult.DiskWriteRate,
                Session_->BestRoundDuration);

            SetResult(Session_->BestRoundResult);
            Stop();
        }
    }

    void SetResult(const TStoreLocation::TIOStatistics& result)
    {
        DiskReadCapacity_.Update(result.DiskReadRate);
        DiskWriteCapacity_.Update(result.DiskWriteRate);

        auto guard = Guard(SpinLock_);
        LastMeasuredThroughtput_ = result;
        LastFinishedTime_ = TInstant::Now();
    }
};

using TLocationLoadTesterPtr = TIntrusivePtr<TLocationLoadTester>;

////////////////////////////////////////////////////////////////////////////////

static const auto SuncStateInterval = TDuration::Seconds(5);

class TIOThroughputMeter
    : public IIOThroughputMeter
{
public:
    explicit TIOThroughputMeter(
        TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TChunkStorePtr chunkStore,
        NLogging::TLogger logger)
        : DynamicConfigManager_(std::move(dynamicConfigManager))
        , ChunkStore_(std::move(chunkStore))
        , Logger(std::move(logger))
        , ActionQueue_(New<TActionQueue>("IOThroughputMeter"))
        , Invoker_(ActionQueue_->GetInvoker())
        , ProbesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TIOThroughputMeter::SyncState, MakeWeak(this)),
            SuncStateInterval))
    {
        for (const auto& location : ChunkStore_->Locations()) {
            Locations_[location->GetUuid()] = New<TLocationLoadTester>(
                ChunkStore_,
                location,
                Invoker_,
                Logger);
        }

        ProbesExecutor_->Start();
    }

    TIOCapacity GetLocationIOCapacity(TChunkLocationUuid id) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto it = Locations_.find(id);
        if (it == Locations_.end()) {
            YT_LOG_WARNING("IO capacity requested for unknown location (LocationUUID: %v)", id);
            return {};
        }
        auto capacity = it->second->GetMeasured();
        return TIOCapacity{
            .DiskReadCapacity = capacity.DiskReadRate,
            .DiskWriteCapacity = capacity.DiskWriteRate,
        };
    }

private:
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const TChunkStorePtr ChunkStore_;
    const NLogging::TLogger Logger;

    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr ProbesExecutor_;

    THashMap<TChunkLocationUuid, TLocationLoadTesterPtr> Locations_;


    void SyncState()
    {
        SetRandomSeed(GetCpuInstant());

        VERIFY_INVOKER_AFFINITY(Invoker_);
        auto config = DynamicConfigManager_->GetConfig()->DataNode->IOThroughputMeter;

        for (auto& [_, location] : Locations_) {
            auto mediumConfig = GetMediumConfig(config, location->GetMediumName());

            YT_LOG_WARNING("Sync state (Enabled: %v, Medium: %v, MediumsSize: %v)",
                config->Enabled,
                location->GetMediumName(),
                config->Mediums.size());

            if (!config->Enabled || !mediumConfig || !mediumConfig->Enabled) {
                Stop(location);
                continue;
            }

            auto scheduled = location->GetScheduledTime();
            if (!scheduled) {
                Schedule(location, config);
                continue;
            }

            if (TInstant::Now() > scheduled) {
                RunTest(location, config, mediumConfig);
                continue;
            }

            if (location->GetRunningTime() > config->TestingTimeHardLimit) {
                location->Stop();
            }
        }
    }

    TMediumThroughputMeterConfigPtr GetMediumConfig(
        TIOThroughputMeterConfigPtr config,
        const TString& medium)
    {
        for (const auto& mediumConfig : config->Mediums) {
            if (mediumConfig->MediumName == medium) {
                return mediumConfig;
            }
        }
        return {};
    }

    void Schedule(const TLocationLoadTesterPtr& location, const TIOThroughputMeterConfigPtr& config)
    {
        auto period = config->TimeBetweenTests;
        auto runAfter = period / 2 + TDuration::Seconds(RandomNumber(period.Seconds()));
        location->SetScheduledTime(TInstant::Now() + runAfter);
    }

    void RunTest(
        const TLocationLoadTesterPtr& location,
        const TIOThroughputMeterConfigPtr& config,
        const TMediumThroughputMeterConfigPtr& mediumConfig)
    {
        if (!location->Running()) {
            location->Run(config, mediumConfig);
        }
    }

    void Stop(const TLocationLoadTesterPtr& locationTest)
    {
        locationTest->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOThroughputMeterPtr CreateIOThroughputMeter(
    TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    NLogging::TLogger logger)
{
    return New<TIOThroughputMeter>(
        std::move(dynamicConfigManager),
        std::move(chunkStore),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
