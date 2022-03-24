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

class TRandomReader
    : public TRefCounted
{
public:
    TRandomReader(
        TGentleLoaderConfigPtr config,
        IRandomFileProviderPtr fileProvider,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , FileProvider(fileProvider)
        , Logger(std::move(logger))
    {
        OpenNewFiles();
    }

    void OpenNewFiles()
    {
        std::vector<TFuture<TReadFileInfo>> futures;
        for (int i = 0; i < Config_->ReadersCount; ++i) {
            auto file = FileProvider->GetRandomExistingFile();
            if (!file) {
                continue;
            }

            auto mode = OpenExisting | RdOnly | CloseOnExec;
            if (Config_->UseDirectIO) {
                mode |= DirectAligned;
            }

            auto future = IOEngine_->Open({file->Path, mode})
                .Apply(BIND([file] (const TIOEngineHandlePtr& handle) {
                    return TReadFileInfo{
                        .Handle = handle,
                        .FileSize = file->DiskSpace,
                    };
                }));
            futures.push_back(future);
        }

        auto results = WaitFor(AllSet(futures))
            .Value();

        std::vector<TReadFileInfo> filesToRead;
        for (const auto& result : results) {
            if (result.IsOK()) {
                filesToRead.push_back(result.Value());
            }
        }

        std::swap(filesToRead, FilesToRead_);
    }

    TFuture<TDuration> Read(i64 readSize)
    {
        if (FilesToRead_.empty()) {
            return MakeFuture<TDuration>(TError("No files to read."));
        }
        auto index = RandomNumber<ui32>(FilesToRead_.size());
        return DoRandomRead(FilesToRead_[index], readSize);
    }

private:
    struct TReadFileInfo
    {
        TIOEngineHandlePtr Handle;
        i64 FileSize = 0;
    };

    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const IRandomFileProviderPtr FileProvider;
    const NLogging::TLogger Logger;
    std::vector<TReadFileInfo> FilesToRead_;

    TFuture<TDuration> DoRandomRead(
        TReadFileInfo fileInfo,
        i64 readSize)
    {
        i64 offset = 0;
        if (fileInfo.FileSize > (readSize + PageSize)) {
            auto maxPageIndex = (fileInfo.FileSize - readSize) / PageSize;
            offset = RandomNumber<ui32>(maxPageIndex) * PageSize;
        } else {
            readSize = PageSize;
        }

        if (readSize + offset > fileInfo.FileSize) {
            return MakeFuture<TDuration>(TError("Wrong packets size")
                << TErrorAttribute("read_size", readSize)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("disk_space", fileInfo.FileSize));
        }

        struct TChunkFileReaderBufferTag
        { };

        NProfiling::TWallTimer requestTimer;
        return IOEngine_->Read<TChunkFileReaderBufferTag>(
            {{fileInfo.Handle, offset, readSize}},
            EWorkloadCategory::Idle)
            .AsVoid()
            .Apply(BIND([requestTimer] {
                return requestTimer.GetElapsedTime();
            })).ToUncancelable();
    }
};

using TRandomReaderPtr = TIntrusivePtr<TRandomReader>;

////////////////////////////////////////////////////////////////////////////////

class TRandomWriter
    : public TRefCounted
{
public:
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
        if (current == ECongestedStatus::OK) {
            FailedProbesCounter_ = 0;
        } else {
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
        TRandomReaderPtr randomReader,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , Logger(std::move(logger))
        , IOEngine_(std::move(engine))
        , Invoker_(std::move(invoker))
        , RandomReader_(std::move(randomReader))
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

        if (std::ssize(Probes_) > Config_->MaxInFlightProbeCount / 2) {
            return ECongestedStatus::HeavyOverload;
        }

        std::vector<TFuture<TDuration>> lastProbes;
        lastProbes.assign(Probes_.rbegin(), Probes_.rbegin() + Config_->ProbesPerRound);
        auto results = WaitFor(AllSetWithTimeout(lastProbes, Config_->ProbeDeadline))
            .Value();

        int failedRequestCount = 0;
        for (const auto& result : results) {
            if (!result.IsOK()) {
                YT_LOG_DEBUG(result, "Probe is failed");
            }
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
    const TRandomReaderPtr RandomReader_;
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

            if (std::ssize(Probes_) < Config_->MaxInFlightProbeCount) {
                Probes_.push_back(RandomReader_->Read(Config_->PacketSize));
            } else {
                YT_LOG_ERROR("Something went wrong: max probe request count reached");
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
        TRandomReaderPtr randomReader,
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
            randomReader,
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

class TLoadAdjuster
{
public:
    TLoadAdjuster(
        TGentleLoaderConfigPtr config,
        IIOEngineWorkloadModelPtr engine,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , IOEngine_(std::move(engine))
        , Logger(std::move(logger))
    { }

    bool ShouldSkipRead(i64 size)
    {
        Adjust();
        SyntheticReadBytes_ += size;
        return RandomNumber<double>() < SkipReadProbability_;
    }

    bool ShouldSkipWrite(i64 size)
    {
        Adjust();
        SyntheticWrittenBytes_ += size;
        return RandomNumber<double>() < SkipWriteProbability_;
    }

    void ResetStatistics()
    {
        LastTotalReadBytes_ = IOEngine_->GetTotalReadBytes();
        LastTotalWrittenBytes_ = IOEngine_->GetTotalWrittenBytes();
    }

private:
    const TGentleLoaderConfigPtr Config_;
    const IIOEngineWorkloadModelPtr IOEngine_;
    const NLogging::TLogger Logger;

    TInstant LoadAdjustedAt_;
    i64 SyntheticWrittenBytes_ = 0;
    i64 SyntheticReadBytes_ = 0;
    i64 LastTotalReadBytes_ = 0;
    i64 LastTotalWrittenBytes_ = 0;

    double SkipWriteProbability_ = 0;
    double SkipReadProbability_ = 0;

    void Adjust()
    {
        auto now = TInstant::Now();
        if (now - LoadAdjustedAt_ < Config_->LoadAdjustingInterval) {
            return;
        }

        auto totalReadBytes = IOEngine_->GetTotalReadBytes();
        auto totalWrittenBytes = IOEngine_->GetTotalWrittenBytes();

        i64 totalReadDelta = totalReadBytes - LastTotalReadBytes_;
        i64 totalWrittenDelta = totalWrittenBytes - LastTotalWrittenBytes_;

        YT_VERIFY(totalReadDelta >= 0);
        YT_VERIFY(totalWrittenDelta >= 0);

        SkipReadProbability_ = CalculateSkipProbability(totalReadDelta, SyntheticReadBytes_);
        SkipWriteProbability_ = CalculateSkipProbability(totalWrittenDelta, SyntheticWrittenBytes_);

        // These are excessive load that we can't mitigate with the next round.
        i64 readExtraDebt = std::max<i64>(0, totalReadDelta - SyntheticReadBytes_);
        i64 writeExtraDebt = std::max<i64>(0, totalWrittenDelta - SyntheticWrittenBytes_);

        YT_LOG_DEBUG("Adjusting for background load "
            "(TotalRead: %v, TotalWritten: %v, SyntheticRead: %v, SyntheticWritten: %v, "
            "ReadSkip: %v, WriteSkip: %v, ReadDebt: %v, WriteDebt: %v)",
            totalReadDelta,
            totalWrittenDelta,
            SyntheticReadBytes_,
            SyntheticWrittenBytes_,
            SkipReadProbability_,
            SkipWriteProbability_,
            readExtraDebt,
            writeExtraDebt);

        SyntheticReadBytes_ = 0;
        SyntheticWrittenBytes_ = 0;
        LastTotalReadBytes_ = totalReadBytes - readExtraDebt;
        LastTotalWrittenBytes_ = totalWrittenBytes - writeExtraDebt;

        YT_VERIFY(LastTotalReadBytes_ >= 0);
        YT_VERIFY(LastTotalWrittenBytes_ >= 0);

        LoadAdjustedAt_ = now;
    }

    static double CalculateSkipProbability(i64 totalBytes, i64 syntheticTargetBytes)
    {
        auto value = static_cast<double>(totalBytes - syntheticTargetBytes) / (syntheticTargetBytes + 1);
        value = std::max(value, 0.0);
        value = std::min(value, 1.0);
        return value;
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
        , ReadToWriteRatio_(GetReadToWriteRatio(Config_, IOEngine_))
        , LoadAdjuster_(Config_, IOEngine_, Logger)
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
    const ui8 ReadToWriteRatio_;
    
    TLoadAdjuster LoadAdjuster_;
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

        auto randomWriter = New<TRandomWriter>(
            Config_,
            LocationRoot_,
            IOEngine_,
            Logger);

        auto randomReader = New<TRandomReader>(
            Config_,
            RandomFileProvider_,
            IOEngine_,
            Logger);

        auto congestionDetector = New<TCongestionDetector>(
            Config_->CongestionDetector,
            IOEngine_,
            Invoker_,
            randomReader,
            Logger);

        const i32 initialWindow = Config_->SegmentSize;

        // State variable that limits the amount of data to send to medium.
        i32 congestionWindow = initialWindow;

        // State variable determines whether the slow start or congestion avoidance algorithm
        // is used to control data transmission
        i32 slowStartThreshold = Config_->MaxWindowSize;

        TCongestedState lastState;
        ui64 requestsCounter = 0;
        LoadAdjuster_.ResetStatistics();

        while (Started_) {
            try {
                SendRequest(congestionWindow, *randomReader, *randomWriter);
                ++requestsCounter;

                auto state = congestionDetector->GetState();
                if (state.Epoch == lastState.Epoch) {
                    continue;
                }
                lastState = state;
                auto prevWindow = congestionWindow;

                randomReader->OpenNewFiles();

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
                    WaitAfterCongested(congestionDetector);
                    lastState = congestionDetector->GetState();
                    LoadAdjuster_.ResetStatistics();
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Gentle loader loop failed");
            }
        }
    }

    void WaitAfterCongested(TIntrusivePtr<TCongestionDetector> congestionDetector)
    {
        YT_LOG_DEBUG("Before waiting for all requests");
        auto wait = WaitFor(AllSet(Results_));
        Y_UNUSED(wait);

        static const int MaxWaitAttempt = 10;
        for (int attempt = 0; attempt < MaxWaitAttempt; ++attempt) {
            YT_LOG_DEBUG("Waiting after congested (Duration: %v, Attempt: %v)",
                Config_->WaitAfterCongested,
                attempt);

            TDelayedExecutor::WaitForDuration(Config_->WaitAfterCongested);
            auto state = congestionDetector->GetState();
            if (state.Status == ECongestedStatus::OK) {
                break;
            }
        }
    }

    void SendRequest(i32 congestionWindow, TRandomReader& reader, TRandomWriter& writer)
    {
        if (std::ssize(Results_) >= Config_->MaxInFlightCount) {
            auto _ = WaitFor(AnySet(Results_));
            Results_.erase(std::remove_if(
                Results_.begin(),
                Results_.end(),
                [&] (const TFuture<TDuration>& future) {
                    if (future.IsSet() && !future.Get().IsOK()) {
                        YT_LOG_DEBUG(future.Get(), "Request is failed");
                    }
                    return future.IsSet();
                }),
                Results_.end());
        }

        static const ui8 IOScale = 100;
        TFuture<TDuration> future = ReadToWriteRatio_ > RandomNumber(IOScale)
            ? SendReadRequest(reader)
            : SendWriteRequest(writer);

        if (future) {
            // Slowing down IOEngine executor for testing purposes.
            if (Config_->SimulatedRequestLatency) {
                auto simulatedLatency = Config_->SimulatedRequestLatency;
                future = future.Apply(BIND([simulatedLatency] (TDuration duration) {
                    Sleep(simulatedLatency);
                    return duration + simulatedLatency;
                }));
            }

            Results_.push_back(std::move(future));
        }

        TDuration yieldTimeout = TDuration::Seconds(1) / congestionWindow;
        TDelayedExecutor::WaitForDuration(yieldTimeout);
    }

    TFuture<TDuration> SendReadRequest(TRandomReader& reader)
    {
        auto readSize = Config_->PacketSize;

        if (LoadAdjuster_.ShouldSkipRead(readSize)) {
            // Adjusting for background workload.
            return {};
        }

        return reader.Read(readSize);
    }

    TFuture<TDuration> SendWriteRequest(TRandomWriter& writer)
    {
        if (LoadAdjuster_.ShouldSkipWrite(Config_->PacketSize)) {
            // Adjusting for background workload.
            return {};
        }

        return writer.Write();
    }

     static ui8 GetReadToWriteRatio(
        const TGentleLoaderConfigPtr& config,
        IIOEngineWorkloadModelPtr engine)
    {
        i64 totalBytes = engine->GetTotalReadBytes() + engine->GetTotalWrittenBytes();

        if (config->AdaptiveReadToWriteRatioThreshold > totalBytes) {
            return config->DefaultReadToWriteRatio;
        }

        return engine->GetTotalReadBytes() * 100 / totalBytes;
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
