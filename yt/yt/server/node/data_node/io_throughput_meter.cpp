#include "io_throughput_meter.h"

#include "chunk_store.h"
#include "blob_chunk.h"
#include "location.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/io/gentle_loader.h>
#include <yt/yt/server/lib/io/io_workload_model.h>

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
    TRandomFileProvider(
        TChunkStorePtr chunkStore,
        TChunkLocationUuid locationUuid,
        i64 minimalFileSize,
        NLogging::TLogger logger)
        : Logger(std::move(logger))
    {
        NProfiling::TWallTimer timer;

        for (const auto& chunk : chunkStore->GetChunks()) {
            auto location = chunk->GetLocation();
            if (chunk->IsActive() || chunk->IsRemoveScheduled() || location->GetUuid() != locationUuid) {
                continue;
            }
            try {
                TFileInfo info{
                    .Path = chunk->GetFileName(),
                    .DiskSpace = NFS::GetPathStatistics(chunk->GetFileName()).Size
                };

                if (info.DiskSpace < minimalFileSize) {
                    continue;
                }
                Chunks_.push_back(std::move(info));
            } catch (const std::exception&) {
            }
        }

        YT_LOG_DEBUG("Loaded chunks (Location: %v, Count: %v, ElapsedTime: %v)",
            locationUuid,
            Chunks_.size(),
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

DEFINE_ENUM(ETestingStage,
    ((None) (0))
    ((Estimate) (1))
    ((Verify) (2))
);

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
            Location_->GetId());

        // Override max write rate for current location.
        mediumConfig = CloneYsonStruct(mediumConfig);
        if (auto dwpd = Location_->GetMaxWriteRateByDwpd()) {
            mediumConfig->MaxWriteRate = static_cast<i64>(dwpd * mediumConfig->DWPDFactor);

            YT_LOG_DEBUG("Setting MaxWriteRate for io test (Location: %v, DWPD: %v, DWPDFactor: %v, MaxWriteRate: %v)",
                Location_->GetId(),
                dwpd,
                mediumConfig->DWPDFactor,
                mediumConfig->MaxWriteRate);
        }

        if (mediumConfig->MaxWriteRate == 0) {
            YT_LOG_WARNING("Skipping load test for location. DWPD of current disk is unknown (Location: %v)",
                Location_->GetId());

            Stop();
            return;
        }

        auto now = TInstant::Now();
        Session_ = TSession{
            .Timestamp = now,
            .LastCongested = now,
            .Config = config,
            .MediumConfig = mediumConfig,
        };

        StartTest(ETestingStage::Estimate);
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
                Location_->GetId());
        }

        if (session && session->Loader) {
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
            Location_->GetId(),
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
        return LastMeasuredThroughput_;
    }

    TString GetRootPath() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return Location_->GetPath();
    }

    TString GetId() const
    {
        return Location_->GetId();
    }

private:

    struct TSession
    {
        NIO::IGentleLoaderPtr Loader;
        TInstant Timestamp;
        TInstant LastCongested;

        ETestingStage Stage = ETestingStage::None;

        TIOThroughputMeterConfigPtr Config;
        TMediumThroughputMeterConfigPtr MediumConfig;

        TDuration BestRoundDuration;
        i64 BestRoundCongestionWindow = 0;
        int RoundsCount = 0;
    };

    const TChunkStorePtr ChunkStore_;
    const TStoreLocationPtr Location_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    std::optional<TSession> Session_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TStoreLocation::TIOStatistics LastMeasuredThroughput_;
    TInstant LastFinishedTime_ = TInstant::Now();

    NProfiling::TGauge DiskReadCapacity_;
    NProfiling::TGauge DiskWriteCapacity_;

    std::optional<TInstant> ScheduledAt_;

    void StartTest(ETestingStage stage)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!Session_) {
            return;
        }

        YT_LOG_DEBUG("Starting test stage (Location: %v, Stage: %v)",
            Location_->GetId(),
            stage);

        auto config = NYTree::CloneYsonStruct(Session_->MediumConfig);
        if (stage == ETestingStage::Verify) {
            auto window = Session_->BestRoundCongestionWindow;
            config->SegmentSize = static_cast<i64>(window * config->VerificationSegmentSizeFactor) + 1;
            config->InitialWindowSize = static_cast<i64>(window * config->VerificationInitialWindowFactor) + 1;
            config->InitialSlowStartThreshold = config->InitialWindowSize;
            config->WindowPeriod = config->VerificationWindowPeriod;

            YT_LOG_DEBUG("Verification stage parameters (Location: %v, SegmentSize: %v, InitialWindowSize: %v, "
                "InitialSlowStartThreshold: %v, WindowPeriod: %v)",
                Location_->GetId(),
                config->SegmentSize,
                config->InitialWindowSize,
                config->InitialSlowStartThreshold,
                config->WindowPeriod);
        }

        auto randomFileProvider = New<TRandomFileProvider>(
            ChunkStore_,
            Location_->GetUuid(),
            config->PacketSize,
            Logger);

        auto loader = CreateGentleLoader(
            config,
            Location_->GetPath(),
            Location_->GetIOEngineModel(),
            randomFileProvider,
            Invoker_,
            Logger);

        loader->SubscribeCongested(
            BIND(&TLocationLoadTester::SessionCongested, MakeWeak(this), Session_->Timestamp, stage));

        NIO::TRequestSizes workloadModel;

        if (auto wm = Location_->GetIOEngineModel()->GetRequestSizes(); wm && config->UseWorkloadModel) {
            workloadModel = MakeIdleWorkloadModel(*wm);
        }

        loader->Start(workloadModel);

        Session_->Stage = stage;
        std::swap(Session_->Loader, loader);

        // Stop old task.
        if (loader) {
            loader->Stop();
        }
    }

    NIO::TRequestSizes MakeIdleWorkloadModel(const NIO::TRequestSizes& workloadModel)
    {
        NIO::TRequestSizes resultModel{
            .Duration = workloadModel.Duration,
        };

        for (const auto& histogram : workloadModel.Reads) {
            resultModel.Reads[EWorkloadCategory::Idle] += histogram;
        }

        for (const auto& histogram : workloadModel.Writes) {
            resultModel.Writes[EWorkloadCategory::Idle] += histogram;
        }

        return resultModel;
    }

    void SessionCongested(TInstant sessionTimestamp, ETestingStage stage, i64 congestionWindow)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!Session_
            || Session_->Timestamp != sessionTimestamp
            || Session_->Stage != stage) {
            // Stale event.
            return;
        }

        switch (stage) {
            case ETestingStage::Estimate:
                EstimateCongested(congestionWindow);
                break;

            case ETestingStage::Verify:
                VerifyCongested(congestionWindow);
                break;

            default:
                YT_ABORT();
        }
    }

    void EstimateCongested(i64 congestionWindow)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        const auto& config = Session_->Config;

        // Testing round is defined as a time between two congestions.
        // We treat each round as independent testing attempt and favor result
        // with longer duration.
        // Due nature of congestion control algorithm (additive increase) and
        // modern flash drives (write saturation / steady state after some time)
        // longer test duration correlate with more accurate result.
        auto now = TInstant::Now();
        auto roundDuration = now - Session_->LastCongested;

        if (roundDuration > Session_->BestRoundDuration) {
            Session_->BestRoundDuration = roundDuration;
            Session_->BestRoundCongestionWindow = congestionWindow;
        }

        Session_->LastCongested = now;
        ++Session_->RoundsCount;

        auto estimateDuration = now - Session_->Timestamp;

        if (Session_->RoundsCount > config->MaxEstimateCongestions || estimateDuration > config->EstimateTimeLimit) {
            YT_LOG_DEBUG("Estimate stage finished (Location: %v, EstimateDuration: %v, "
                "RoundsCount: %v, BestRoundDuration: %v, BestRoundWindow: %v)",
                Location_->GetId(),
                estimateDuration,
                Session_->RoundsCount,
                Session_->BestRoundDuration,
                Session_->BestRoundCongestionWindow);
            StartTest(ETestingStage::Verify);
        }
    }

    void VerifyCongested(i64 /*congestionWindow*/)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto statistics = Location_->GetIOStatistics();

        // Use filesystem level statistics if disk stats are not available.
        // This is mostly for test environment.
        if (statistics.DiskReadRate == 0 && statistics.DiskWriteRate == 0) {
            statistics.DiskReadRate = statistics.FilesystemReadRate;
            statistics.DiskWriteRate = statistics.FilesystemWriteRate;
        }

        auto now = TInstant::Now();
        auto overallDuration = now - Session_->Timestamp;
        auto verifyStageDuration = now - Session_->LastCongested;

        YT_LOG_WARNING("Setting test results (Location: %v, OverallDuration: %v, VerifyStageDuration: %v, "
            "DiskReadRate: %v, DiskWriteRate: %v)",
            Location_->GetId(),
            overallDuration,
            verifyStageDuration,
            statistics.DiskReadRate,
            statistics.DiskWriteRate);

        SetResult(statistics);
        Stop();
    }

    void SetResult(const TStoreLocation::TIOStatistics& result)
    {
        DiskReadCapacity_.Update(result.DiskReadRate);
        DiskWriteCapacity_.Update(result.DiskWriteRate);

        auto guard = Guard(SpinLock_);
        LastMeasuredThroughput_ = result;
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
    TIOThroughputMeter(
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
                Logger.WithTag("Location:%v", location->GetId()));
        }

        ProbesExecutor_->Start();
    }

    TIOCapacity GetLocationIOCapacity(TChunkLocationUuid uuid) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto it = Locations_.find(uuid);
        if (it == Locations_.end()) {
            YT_LOG_WARNING("IO capacity requested for unknown location (LocationUUID: %v)", uuid);
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

        // Cancel all scheduled/ongoing test for locations that should not be tested anymore.
        for (auto& [_, location] : Locations_) {
            auto mediumConfig = GetMediumConfig(config, location->GetMediumName());

            if (!config->Enabled || !mediumConfig || !mediumConfig->Enabled) {
                location->Stop();
                continue;
            }

            if (location->Running() && location->GetRunningTime() > config->TestingTimeHardLimit) {
                YT_LOG_WARNING("Cancel stuck tests (Location: %v, RunningTime: %v, TestingTimeHardLimit: %v)",
                    location->GetId(),
                    location->GetRunningTime(),
                    config->TestingTimeHardLimit);
                location->Stop();
            }
        }

        // Schedule throughput tests if needed.
        if (!AnyScheduled()) {
            ScheduleAll(config);
            return;
        }

        // Run tests one by one.
        if (!AnyRunning()) {
            RunOne(config);
        }
    }

    bool AnyScheduled()
    {
        for (auto& [_, location] : Locations_) {
            if (location->GetScheduledTime()) {
                return true;
            }
        }
        return false;
    }

    bool AnyRunning()
    {
        for (auto& [_, location] : Locations_) {
            if (location->Running()) {
                return true;
            }
        }
        return false;
    }

    TMediumThroughputMeterConfigPtr GetMediumConfig(
        TIOThroughputMeterConfigPtr config,
        const TString& medium)
    {
        for (const auto& mediumConfig : config->Media) {
            if (mediumConfig->MediumName == medium) {
                return mediumConfig;
            }
        }
        return {};
    }

    void VisitEnabledLocation(const TIOThroughputMeterConfigPtr& config, auto visitor) {
        for (auto& [_, location] : Locations_) {
            auto mediumConfig = GetMediumConfig(config, location->GetMediumName());
            if (!config->Enabled || !mediumConfig || !mediumConfig->Enabled) {
                continue;
            }
            if (!visitor(location, mediumConfig)) {
                break;
            }
        }
    }

    void ScheduleAll(const TIOThroughputMeterConfigPtr& config)
    {
        VisitEnabledLocation(config, [&] (const TLocationLoadTesterPtr& location, const TMediumThroughputMeterConfigPtr&) {
            auto scheduled = location->GetScheduledTime();
            YT_VERIFY(!scheduled);

            auto period = config->TimeBetweenTests;
            auto runAfter = period / 2 + TDuration::Seconds(RandomNumber(period.Seconds()));
            location->SetScheduledTime(TInstant::Now() + runAfter);
            return true;
        });
    }

    void RunOne(const TIOThroughputMeterConfigPtr& config)
    {
        VisitEnabledLocation(config, [&] (const TLocationLoadTesterPtr& location, const TMediumThroughputMeterConfigPtr& mediumConfig) {
            YT_VERIFY(!location->Running());

            auto scheduled = location->GetScheduledTime();
            if (scheduled && TInstant::Now() > scheduled) {
                location->Run(config, mediumConfig);
                // Signal not to continue.
                return false;
            }
            return true;
        });
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
