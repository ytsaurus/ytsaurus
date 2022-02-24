#include "gentle_loader.h"
#include "config.h"
#include "io_workload_model.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT::NIO {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const int PageSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

TFuture<TDuration> DoRandomRead(
    IIOEnginePtr engine,
    IRandomFileProviderPtr fileProvider,
    i64 readSize,
    bool directIO)
{
    auto file = fileProvider->GetRandomExistingFile();
    if (!file) {
        return MakeFuture<TDuration>(TError("Can not get file to read"));
    }

    i64 offset = 0;
    if (file->DiskSpace > (readSize + PageSize)) {
        auto maxPageIndex = (file->DiskSpace - readSize) / PageSize;
        offset = RandomNumber<ui32>(maxPageIndex) * PageSize;
    } else {
        readSize = PageSize;
    }

    if (readSize + offset > file->DiskSpace) {
        return MakeFuture<TDuration>(TError("Wrong packets size")
            << TErrorAttribute("read_size", readSize)
            << TErrorAttribute("offset", offset)
            << TErrorAttribute("disk_space", file->DiskSpace));
    }

    struct TChunkFileReaderBufferTag
    { };

    auto mode = OpenExisting | RdOnly | CloseOnExec;
    if (directIO) {
        mode |= DirectAligned;
    }

    return engine->Open({file->Path, mode})
        .ToUncancelable()
        .Apply(BIND([engine, offset, readSize] (const TIOEngineHandlePtr& dataFile) {
            NProfiling::TWallTimer requestTimer;
            return engine->Read<TChunkFileReaderBufferTag>(
                {{dataFile, offset, readSize}},
                EWorkloadCategory::Idle)
                .AsVoid()
                .Apply(BIND([engine, dataFile, requestTimer] {
                    auto duration = requestTimer.GetElapsedTime();
                    return engine->Close({
                            .Handle = std::move(dataFile)
                        })
                        .Apply(BIND([duration] () {
                            return duration;
                        }));
                }));
        }));
}

////////////////////////////////////////////////////////////////////////////////

struct TRandomWriter
    : public TRefCounted
{
    TRandomWriter(
        TGentleLoaderConfigPtr config,
        const TString& path,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , Logger(std::move(logger))
        , WriteThreadPool_(New<TThreadPool>(4, "RandomWriter"))
        , Buffer_(MakeRandomBuffer(Config_->PacketSize))
    {
        const TString tempFilesDir = NFS::JoinPaths(path, Config_->WritersFolder);
        if (!NFS::Exists(tempFilesDir)) {
            NFS::MakeDirRecursive(tempFilesDir);
        }

        NFS::CleanTempFiles(tempFilesDir);
        auto invoker = WriteThreadPool_->GetInvoker();

        for (int i = 0; i < Config_->WritersCount; ++i) {
            auto& info = FilesToWrite_.emplace_back();
            info.FilePath = NFS::JoinPaths(tempFilesDir, Format("%v%v", i, NFS::TempFileSuffix));

            EOpenMode mode = CreateAlways | WrOnly | CloseOnExec;
            if (Config_->UseDirectIO) {
                mode |= DirectAligned;
            }

            info.Handle = WaitFor(IOEngine_->Open({info.FilePath, mode}))
                .ValueOrThrow();
            info.Invoker = NConcurrency::CreateSerializedInvoker(invoker);
        }
    }

    ~TRandomWriter()
    {
        for (auto& info : FilesToWrite_) {
            try {
                info.Handle->Close();
                NFS::Remove(info.FilePath);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to close benchmark file (FilePath: %v)", info.FilePath);
            }
        }
    }

    TFuture<TDuration> Write()
    {
        auto index = RandomNumber<ui32>(FilesToWrite_.size());
        const auto& fileInfo = FilesToWrite_[index];
        return BIND(&TRandomWriter::DoWrite, MakeStrong(this), index)
            .AsyncVia(fileInfo.Invoker)
            .Run();
    }

private:
    struct TFileInfo
    {
        TString FilePath;
        TIOEngineHandlePtr Handle;
        i64 Offset = 0;
        IInvokerPtr Invoker;
    };

    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const NLogging::TLogger Logger;
    const TThreadPoolPtr WriteThreadPool_;
    const TSharedRef Buffer_;

    std::vector<TFileInfo> FilesToWrite_;


    TDuration DoWrite(ui32 index)
    {
        auto& fileInfo = FilesToWrite_[index];

        VERIFY_INVOKER_AFFINITY(fileInfo.Invoker);

        NProfiling::TWallTimer requestTimer;
        auto future = IOEngine_->Write({
            fileInfo.Handle,
            fileInfo.Offset,
            {Buffer_},
        });

        WaitFor(future)
            .ThrowOnError();

        fileInfo.Offset += Config_->PacketSize;
        if (fileInfo.Offset >= Config_->MaxWriteFileSize) {
            fileInfo.Offset = 0;
        }

        return requestTimer.GetElapsedTime();
    }

    static TSharedMutableRef MakeRandomBuffer(i32 size)
    {
        auto data = TSharedMutableRef::AllocatePageAligned(size, false);
        for (int index = 0; index < size; ++index) {
            data[index] = RandomNumber<ui8>();
        }
        return data;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECongestedStatus, i16,
    ((OK)               (0))
    ((Overload)         (1))
    ((HeavyOverload)    (2))
);

struct TCongestedState
{
    ui16 Epoch = 0;
    ECongestedStatus Status = ECongestedStatus::OK;
};

////////////////////////////////////////////////////////////////////////////////

using TAtomicCongestedState = std::atomic<TCongestedState>;
static_assert(TAtomicCongestedState::is_always_lock_free);

class TInteractiveRequestsObserver
    : public TRefCounted
{
public:
    TInteractiveRequestsObserver(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
    { }

    ECongestedStatus GetCongestionStatus()
    {
        auto current = GetCurrentCongestionStatus();
        if (current != ECongestedStatus::OK) {
            ++FailedProbesCounter_;
        }

        if (FailedProbesCounter_ >= Config_->UserRequestFailedProbesThreshold) {
            return current;
        }
        return ECongestedStatus::OK;
    }

private:
    const TCongestionDetectorConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;

    i64 FailedProbesCounter_ = 0;

    ECongestedStatus GetCurrentCongestionStatus()
    {
        auto latencyHistogram = IOEngine_->GetRequestLatencies();
        if (!latencyHistogram) {
            return ECongestedStatus::OK;
        }

        const auto& userReadLatency = latencyHistogram->Reads[EWorkloadCategory::UserInteractive];
        auto quantiles = ComputeHistogramSumary(userReadLatency);
        auto q99Latency = TDuration::MilliSeconds(quantiles.P99);

        YT_LOG_DEBUG("User interactive request congestion status (Count: %v, 90p: %v ms, 99p: %v ms, 99.9p: %v ms)",
            quantiles.TotalCount,
            quantiles.P90,
            quantiles.P99,
            quantiles.P99_9);

        if (q99Latency > Config_->UserRequestHeavyOverloadThreshold) {
            return ECongestedStatus::HeavyOverload;
        } else if (q99Latency > Config_->UserRequestOverloadThreshold) {
            return ECongestedStatus::Overload;
        }

        return ECongestedStatus::OK;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProbesCongestionDetector
    : public TRefCounted
{
public:
    TProbesCongestionDetector(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        IInvokerPtr invoker,
        IRandomFileProviderPtr fileProvider,
        bool useDirectIO,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
        , Invoker_(std::move(invoker))
        , RandomFileProvider_(std::move(fileProvider))
        , UseDirectIO(useDirectIO)
        , ProbesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TProbesCongestionDetector::DoProbe, MakeWeak(this)),
            Config_->ProbesInterval))
    {
        ProbesExecutor_->Start();
    }

    ~TProbesCongestionDetector()
    {
        ProbesExecutor_->Stop();
    }

    ECongestedStatus GetCongestionStatus()
    {
        if (std::ssize(Probes_) < Config_->ProbesPerRound) {
            return ECongestedStatus::OK;
        }

        if (std::ssize(Probes_) > Config_->MaxInflightProbesCount / 2) {
            return ECongestedStatus::HeavyOverload;
        }

        std::vector<TFuture<TDuration>> lastProbes;
        lastProbes.assign(Probes_.rbegin(), Probes_.rbegin() + Config_->ProbesPerRound);
        auto results = WaitFor(AllSetWithTimeout(lastProbes, Config_->ProbeDeadline))
            .Value();

        int failedRequestCount = 0;
        for (const auto& result : results) {
            if (!result.IsOK() || result.Value() > Config_->ProbeDeadline) {
                ++failedRequestCount;
            }
        }
        int failedRequestsPercentage = failedRequestCount * 100 / lastProbes.size();

        YT_LOG_DEBUG("Probes congested status (Count: %v, Failed: %v, Percentage: %v)",
            lastProbes.size(),
            failedRequestCount,
            failedRequestsPercentage);

        if (failedRequestsPercentage > Config_->HeavyOverloadThreshold) {
            return ECongestedStatus::HeavyOverload;
        } else if (failedRequestsPercentage > Config_->OverloadThreshold) {
            return ECongestedStatus::Overload;
        }
        return ECongestedStatus::OK;
    }

private:
    const TCongestionDetectorConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IInvokerPtr Invoker_;
    const IRandomFileProviderPtr RandomFileProvider_;
    const bool UseDirectIO;
    const TPeriodicExecutorPtr ProbesExecutor_;

    std::deque<TFuture<TDuration>> Probes_;

    void DoProbe()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            // Dropping old data.
            while (std::ssize(Probes_) > Config_->ProbesPerRound && Probes_.front().IsSet()) {
                Probes_.pop_front();
            }

            if (std::ssize(Probes_) < Config_->MaxInflightProbesCount) {
                Probes_.push_back(DoRandomRead(
                    IOEngine_,
                    RandomFileProvider_,
                    Config_->PacketSize,
                    UseDirectIO));
            } else {
                YT_LOG_ERROR("Something went wrong: max probes request count reached");
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Congestion detector probes failed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCongestionDetector
    : public TRefCounted
{
public:
    TCongestionDetector(
        TCongestionDetectorConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        IInvokerPtr invoker,
        IRandomFileProviderPtr fileProvider,
        bool useDirectIO,
        NLogging::TLogger logger)
        : Invoker_(std::move(invoker))
        , ProbesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCongestionDetector::DoProbe, MakeWeak(this)),
            config->ProbesInterval * config->ProbesPerRound))
    {
        InteractiveObserver_ = New<TInteractiveRequestsObserver>(config, engine, logger);
        ProbesObserver_ = New<TProbesCongestionDetector>(
            config,
            engine,
            Invoker_,
            fileProvider,
            useDirectIO,
            logger);

        ProbesExecutor_->Start();
    }

    ~TCongestionDetector()
    {
        ProbesExecutor_->Stop();
    }

    TCongestedState GetState() const
    {
        return State_.load(std::memory_order_relaxed);
    }

private:
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr ProbesExecutor_;

    ui16 EpochCounter_ = 0;
    TAtomicCongestedState State_ = {};

    TIntrusivePtr<TInteractiveRequestsObserver> InteractiveObserver_;
    TIntrusivePtr<TProbesCongestionDetector> ProbesObserver_;

    void DoProbe()
    {
        TCongestedState state;
        state.Epoch = ++EpochCounter_;

        state.Status = std::max(
            InteractiveObserver_->GetCongestionStatus(),
            ProbesObserver_->GetCongestionStatus());
            
        State_.store(state, std::memory_order_relaxed);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGentleLoader
    : public IGentleLoader
{
public:
    TGentleLoader(
        TGentleLoaderConfigPtr config,
        TString locationRoot,
        IIOEngineWorkloadModelPtr engine,
        IRandomFileProviderPtr fileProvider,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , LocationRoot_(std::move(locationRoot))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
        , RandomFileProvider_(std::move(fileProvider))
        , Invoker_(std::move(invoker))
        , Started_(false)
    { }

    ~TGentleLoader()
    {
        YT_LOG_DEBUG("Destroyed TGentleLoader");
    }

    virtual void Start() override
    {
        YT_VERIFY(!Started_);
        Started_ = true;
        BIND(&TGentleLoader::Run, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual void Stop() override
    {
        YT_VERIFY(Started_);
        Started_ = false;
        YT_LOG_DEBUG("Signaling Gentle loader to stop");
    }

    DEFINE_SIGNAL_OVERRIDE(void(i64 /*currentWindow*/), Congested);

private:
    const TGentleLoaderConfigPtr Config_;
    const TString LocationRoot_;
    const NLogging::TLogger Logger;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IRandomFileProviderPtr RandomFileProvider_;
    const IInvokerPtr Invoker_;

    std::atomic_bool Started_;

    std::vector<TFuture<TDuration>> Results_;

    void Run()
    {
        while (Started_) {
            try {
                DoRun();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Gentle loader run failed");
                TDelayedExecutor::WaitForDuration(Config_->WaitAfterCongested);
            }
        }
    }

    void DoRun()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto congestionDetector = New<TCongestionDetector>(
            Config_->CongestionDetector,
            IOEngine_,
            Invoker_,
            RandomFileProvider_,
            Config_->UseDirectIO,
            Logger);

        auto randomWriter = New<TRandomWriter>(
            Config_,
            LocationRoot_,
            IOEngine_,
            Logger);

        const i32 initialWindow = Config_->SegmentSize;

        // State variable that limits the amount of data to send to medium.
        i32 congestionWindow = initialWindow;

        // State variable determines whether the slow start or congestion avoidance algorithm
        // is used to control data transmission
        i32 slowStartThreshold = Config_->MaxWindowSize;
        
        TCongestedState lastState;

        ui64 requestsCounter = 0;

        while (Started_) {
            try {
                SendRequest(congestionWindow, *randomWriter);
                ++requestsCounter;

                auto state = congestionDetector->GetState();
                if (state.Epoch == lastState.Epoch) {
                    continue;
                }
                lastState = state;
                auto prevWindow = congestionWindow;

                switch (state.Status) {
                    case ECongestedStatus::OK:
                        if (congestionWindow >= slowStartThreshold) {
                            // Congestion avoidance algorithm.
                            congestionWindow += Config_->SegmentSize;
                        } else {
                            // Slow start algorithm
                            // (which is actually quite fast).
                            congestionWindow = std::min(congestionWindow * 2, slowStartThreshold);
                        }
                        break;

                    case ECongestedStatus::Overload:
                        slowStartThreshold = congestionWindow / 2;
                        congestionWindow = slowStartThreshold;
                        break;

                    case ECongestedStatus::HeavyOverload:
                        slowStartThreshold = congestionWindow / 2;
                        congestionWindow = initialWindow;
                        break;

                    default:
                        YT_ABORT();
                }

                // Keeping window in sane limits.
                congestionWindow = std::min(Config_->MaxWindowSize, congestionWindow);
                congestionWindow = std::max(1, congestionWindow);

                YT_LOG_DEBUG("New congestion message received"
                    "(Index: %v, Status: %v, CongestionWindow: %v, SlowStartThreshold: %v, RequestsCounter: %v)",
                    state.Epoch,
                    state.Status,
                    congestionWindow,
                    slowStartThreshold,
                    requestsCounter);

                // If congested wait all inflight events before starting new round.
                if (state.Status != ECongestedStatus::OK) {
                    // Signal congested.
                    Congested_.Fire(prevWindow);
                    WaitAfterCongested();
                    lastState = congestionDetector->GetState();
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Gentle loader loop failed");
            }
        }
    }

    void WaitAfterCongested()
    {
        YT_LOG_DEBUG("Before waiting for all requests");
        auto wait = WaitFor(AllSet(Results_));
        Y_UNUSED(wait);
        YT_LOG_DEBUG("Waiting after congested (Duration: %v)",
            Config_->WaitAfterCongested);
        TDelayedExecutor::WaitForDuration(Config_->WaitAfterCongested);
    }

    void SendRequest(i32 congestionWindow, TRandomWriter& writer)
    {
        if (std::ssize(Results_) >= Config_->MaxInFlightCount) {
            auto _ = WaitFor(AnySet(Results_));
            Results_.erase(std::remove_if(
                Results_.begin(), 
                Results_.end(),
                [] (const TFuture<TDuration>& future) {
                    return future.IsSet();
                }),
                Results_.end());
        }

        static const ui8 IOScale = 100;

        TFuture<TDuration> future;
        if (Config_->ReadToWriteRatio > RandomNumber(IOScale)) {
            future = DoRandomRead(
                IOEngine_,
                RandomFileProvider_,
                Config_->PacketSize,
                Config_->UseDirectIO);
        } else {
            future = writer.Write();
        }

        // Slowing down IOEngine executor for testing purposes.
        if (Config_->SimulatedRequestLatency) {
            auto simulatedLatency = Config_->SimulatedRequestLatency;
            future = future.Apply(BIND([simulatedLatency] (TDuration duration) {
                Sleep(simulatedLatency);
                return duration + simulatedLatency;
            }));
        }

        Results_.push_back(std::move(future));
        TDuration yieldTimeout = TDuration::Seconds(1) / congestionWindow;
        TDelayedExecutor::WaitForDuration(yieldTimeout);
    }
};

////////////////////////////////////////////////////////////////////////////////

IGentleLoaderPtr CreateGentleLoader(
    const TGentleLoaderConfigPtr& config,
    const TString& locationRoot,
    IIOEngineWorkloadModelPtr engine,
    IRandomFileProviderPtr fileProvider,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TGentleLoader>(
        std::move(config),
        std::move(locationRoot),
        std::move(engine),
        std::move(fileProvider),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NIO
