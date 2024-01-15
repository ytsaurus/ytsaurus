#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/folder/pathsplit.h>
#include <util/random/fast.h>
#include <util/stream/printf.h>

using namespace NYT::NIO;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NDataNode;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;
using namespace NLastGetopt;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EIOPattern,
    (Read)
    (Write)
    (ReadWrite)
    (RandomRead)
    (RandomWrite)
    (RandomReadWrite)
);

DEFINE_BIT_ENUM(EIOBits,
    ((Read)(1 << 0))
    ((Write)(1 << 1))
    ((Random)(1 << 2))
);

DEFINE_ENUM(EAmmoType,
    (Read)
    (Write)
);

class TTestConfig
    : public TYsonStructLite
{
public:
    EIOEngineType IOEngine;

    EIOPattern IOPattern;
    EIOBits IOBits;

    TString WorkingDirectory;

    i64 BlockSize;
    i64 FileSize;
    int MaxInflight;
    int MaxRps;
    int Threads;
    int PacketSize;
    ui64 PrintInterval;
    const int Alignment = 512;

    INodePtr IOConfig;

    REGISTER_YSON_STRUCT_LITE(TTestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("io_engine", &TThis::IOEngine)
            .Default(EIOEngineType::ThreadPool);
        registrar.Parameter("io_pattern", &TThis::IOPattern)
            .Default(EIOPattern::RandomRead);
        registrar.Parameter("directory", &TThis::WorkingDirectory)
            .Default("/tmp");
        registrar.Parameter("block_size", &TThis::BlockSize)
            .Default(128 * 1024);
        registrar.Parameter("file_size", &TThis::FileSize)
            .Default(512 * 1024 * 1024);
        registrar.Parameter("max_inflight", &TThis::MaxInflight)
            .Default(100);
        registrar.Parameter("max_rps", &TThis::MaxRps)
            .Default(500);
        registrar.Parameter("max_packet", &TThis::PacketSize)
            .Default(10);
        registrar.Parameter("threads", &TThis::Threads)
            .Default(16);
        registrar.Parameter("print_interval_sec", &TThis::PrintInterval)
            .Default(5);
        registrar.Parameter("io_config", &TThis::IOConfig)
            .Optional();
    }

public:
    void Load(NYTree::INodePtr node)
    {
        TYsonStructLite::Load(node);

        switch (IOPattern) {
            case EIOPattern::Read:
                IOBits = EIOBits::Read;
                break;
            case EIOPattern::Write:
                IOBits = EIOBits::Write;
                break;
            case EIOPattern::ReadWrite:
                IOBits = EIOBits::Read | EIOBits::Write;
                break;
            case EIOPattern::RandomRead:
                IOBits = EIOBits::Random | EIOBits::Read;
                break;
            case EIOPattern::RandomWrite:
                IOBits = EIOBits::Random | EIOBits::Write;
                break;
            case EIOPattern::RandomReadWrite:
                IOBits = EIOBits::Read | EIOBits::Write | EIOBits::Random;
                break;
        }
    }
};

struct TAmmo
{
    EAmmoType AmmoType;
    i64 Offset;
    i64 Size;
};

struct TShot
{
    TCpuDuration Duration;
    EAmmoType AmmoType;
    i64 Size;
};

class TBuckets
{
public:
    TBuckets(const std::vector<ui64>& buckets)
        : Buckets_(buckets)
        , Counts_(Buckets_.size())
    { }

    void Update(TCpuDuration cpuDuration)
    {
        auto value = CpuDurationToDuration(cpuDuration).MilliSeconds();
        int total = Buckets_.size();
        for (int index = 0; index < total; ++index) {
            if (value < Buckets_[index]) {
                ++Counts_[index];
                break;
            }
        }
        ++Total_;

        Timings_.push_back(value);
    }

    void Print()
    {
        if (!Total_) {
            return;
        }

        int total = Buckets_.size();
        for (int index = 0; index < total; ++index) {
            if (index == total - 1) {
                Cout << "Inf";
            } else {
                Cout << Buckets_[index];
            }

            Cout << "=" << (Counts_[index] * 100) / Total_ << "%, ";

            Counts_[index] = 0;
        }

        std::sort(Timings_.begin(), Timings_.end());

        for (auto quant : Quantiles_) {
            int index = static_cast<int>(Timings_.size() * quant) - 1;
            auto result = index < 0 ? 0 : Timings_[index];
            Printf(Cout, "%0.3lf%%=%lu, ", quant, result);
        }

        Timings_.clear();

        Total_ = 0;
    }

private:
    std::vector<double> Quantiles_ = {0.75, 0.95, 0.99, 0.995, 0.999, 1.0};
    std::vector<ui64> Buckets_;
    std::vector<size_t> Counts_;
    std::vector<TValue> Timings_;
    size_t Total_ = 0;
};

struct IGaugePrinter
{
    virtual ~IGaugePrinter() = default;
    virtual void PrintMetrics() const = 0;
};

class TStatisticsThread
    : public TSchedulerThread
{
public:
    TStatisticsThread(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        TMpscStack<TShot>& queue,
        ui64 printInterval,
        const IGaugePrinter* gaugePrinter)
        : TSchedulerThread(
            callbackEventCount,
            "ResultProcessing",
            {})
        , Queue_(queue)
        , PrintInterval_(printInterval)
        , GaugePrinter_(gaugePrinter)
    { }

private:
    TMpscStack<TShot>& Queue_;
    ui64 PrintInterval_;
    const IGaugePrinter* GaugePrinter_;

    std::vector<ui64> BucketsBounds_ = { 1, 3, 7, 10, 13, std::numeric_limits<int>::max() };
    TBuckets ReadBuckets_ = BucketsBounds_;
    TBuckets WriteBuckets_ = BucketsBounds_;
    TBuckets TotalBuckets_ = BucketsBounds_;

    size_t BytesRead_ = {0};
    size_t BytesWrite_ = {0};

    TCpuInstant LastPrint_ = GetCpuInstant();

    TClosure BeginExecute() override
    {
        if (Queue_.IsEmpty()) {
            return TClosure();
        }

        return BIND([this] {
            while (Queue_.DequeueAll(true, [&] (const TShot& shot) {
                UpdateStatistics(shot);
            }))
            { }

        });
    }

    void EndExecute() override
    { }

    void UpdateStatistics(const TShot& shot)
    {
        switch (shot.AmmoType) {
            case EAmmoType::Read:
                BytesRead_ += shot.Size;
                ReadBuckets_.Update(shot.Duration);
                break;
            case EAmmoType::Write:
                BytesWrite_ += shot.Size;
                WriteBuckets_.Update(shot.Duration);
                break;
        }

        TotalBuckets_.Update(shot.Duration);
        PrintStastics();
    }

    void PrintStastics()
    {
        auto now = GetCpuInstant();
        auto duration = CpuDurationToDuration(now - LastPrint_).Seconds();
        if (duration >= PrintInterval_)
        {
            Cout << BytesRead_ / 1024 / 1024 << "MiB, "
                 << BytesWrite_ / 1024 / 1024 << "MiB, "
                 << BytesRead_ / duration / 1024 / 1024 << "MiB/s, "
                 << BytesWrite_ / duration / 1024 / 1024  << "MiB/s, ";

            ReadBuckets_.Print();
            WriteBuckets_.Print();
            // TotalBuckets_.Print();

            GaugePrinter_->PrintMetrics();

            Cout << "\n";
            BytesWrite_ = 0;
            BytesRead_ = 0;
            LastPrint_ = now;
        }
    }
};

class TTask
    : public IGaugePrinter
{
public:
    TTask(const TTestConfig& config)
        : Config_(config)
        , IOEngine_(CreateIOEngine(Config_.IOEngine, Config_.IOConfig))
        , RNG_(31337)
        , WorkerPool_(NConcurrency::CreateThreadPool(Config_.Threads, "io_test"))
        , ResultProcessingThread_(New<TStatisticsThread>(
            ShootResultEventCount_,
            ShootResultQueue_,
            Config_.PrintInterval,
            this))
        , Position_(0)
    {
        WriteData_ = TSharedMutableRef::Allocate(Config_.BlockSize + Config_.Alignment, {.InitializeStorage = false});
        WriteData_ = WriteData_.Slice(AlignUp(WriteData_.Begin(), Config_.Alignment), WriteData_.End());
        WriteData_ = WriteData_.Slice(0, Config_.BlockSize);
    }

    void Run()
    {
        PrepareFile();

        File_ = IOEngine_->Open({FileName_, EOpenModeFlag::RdWr})
            .Get()
            .ValueOrThrow();
        Position_ = 0;
        MaxPosition_ = File_->GetLength();

        ResultProcessingThread_->Start();
        Loop();
    }

    void Shutdown()
    {
        WorkerPool_->Shutdown();
        ResultProcessingThread_->Stop();
    }

private:
    const TTestConfig& Config_;
    const IIOEnginePtr IOEngine_;
    TFastRng64 RNG_;

    IThreadPoolPtr WorkerPool_;

    TIntrusivePtr<NThreading::TEventCount> ShootResultEventCount_ = New<NThreading::TEventCount>();
    TMpscStack<TShot> ShootResultQueue_;

    TIntrusivePtr<TStatisticsThread> ResultProcessingThread_;

    // TODO: multiple files
    TString FileName_;
    TIOEngineHandlePtr File_;

    TSharedMutableRef WriteData_;

    std::atomic<int> Inflight_ = {0};

    i64 Position_;
    i64 MaxPosition_;

    void PrintMetrics() const override
    {
        Cout << "fl=" << Inflight_.load(std::memory_order::relaxed) << ", ";
    }

    void PrepareFile()
    {
        TPathSplit path(Config_.WorkingDirectory);
        path.AppendComponent("TestFile");
        FileName_ = path.Reconstruct();

        TFileOutput fileOutput(FileName_);
        TBufferedOutput output(&fileOutput);

        i64 fileSize = 0;

        while (fileSize < Config_.FileSize) {
            auto random = RNG_.GenRand();
            constexpr auto blockSize = sizeof(random);
            output.Write(&random, blockSize);
            fileSize += blockSize;
        }

        MaxPosition_ = fileSize;
        output.Finish();
        fileOutput.Finish();
    }

    void GenerateAmmo(TAmmo* ammo)
    {
        if (Any(Config_.IOBits & EIOBits::Read) && Any(Config_.IOBits & EIOBits::Write)) {
            ammo->AmmoType = (RNG_.GenRand() > (RNG_._Max / 2)) ? EAmmoType::Read : EAmmoType::Write;
        } else if (Any(Config_.IOBits & EIOBits::Write)) {
            ammo->AmmoType = EAmmoType::Write;
        } else {
            ammo->AmmoType = EAmmoType::Read;
        }

        if (Any(Config_.IOBits & EIOBits::Random)) {
            ammo->Offset = AlignDown(RNG_.Uniform(0, MaxPosition_ - Config_.BlockSize), static_cast<ui64>(Config_.Alignment));
        } else {
            ammo->Offset = Position_;
            Position_ += Config_.BlockSize;
            Position_ %= MaxPosition_;
        }
        ammo->Size = Config_.BlockSize;
    }

    void Shoot(std::vector<TAmmo> packet)
    {
        std::vector<TFuture<void>> shots;
        shots.reserve(packet.size());
        for (const auto& ammo : packet) {
            shots.push_back(SingleShoot(ammo));
        }

        AllSet(std::move(shots)).Get();

        -- Inflight_;
    }

    TFuture<void> SingleShoot(const TAmmo& ammo)
    {
        auto t = GetCpuInstant();
        TFuture<void> shot;
        switch (ammo.AmmoType) {
            case EAmmoType::Read:
                struct TIOEngineTestBufferTag
                { };
                shot = IOEngine_->Read(
                    {{File_, ammo.Offset, ammo.Size}},
                    EWorkloadCategory::UserInteractive,
                    GetRefCountedTypeCookie<TIOEngineTestBufferTag>())
                    .As<void>();
                break;
            case EAmmoType::Write:
                shot = IOEngine_->Write({File_, ammo.Offset, {WriteData_}});
                break;
        }

        shot.Subscribe(BIND([t, ammo, this] (const TError& /*unused*/) {
            TShot result;
            result.AmmoType = ammo.AmmoType;
            result.Size = ammo.Size;
            result.Duration = GetCpuInstant() - t;
            ShootResultQueue_.Enqueue(result);
            ShootResultEventCount_->NotifyOne();
        }));

        return shot;
    }

    void Loop()
    {
        while (true) {
            std::vector<TAmmo> packet(Config_.PacketSize);
            for (auto& ammo : packet) {
                GenerateAmmo(&ammo);
            }

            ++ Inflight_;
            YT_UNUSED_FUTURE(BIND(&TTask::Shoot, this, std::move(packet))
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run());

            auto wait = TDuration::MicroSeconds(1000000 / Config_.MaxRps);
            Sleep(wait);

            while (Inflight_ > Config_.MaxInflight) {
                Sleep(wait);
            }
        }
    }
};

void RunTest(const TTestConfig& config) {
    TTask test(config);
    test.Run();
    test.Shutdown();
    Cout <<  "OK\n";
}

void GuardedMain(int argc, char** argv) {
    TOpts opts;
    TString configFileName;
    TTestConfig config;

    opts.AddLongOption("config", "Config")
        .Required()
        .StoreResult(&configFileName);

    TOptsParseResult results(&opts, argc, argv);

    auto node = ConvertTo<INodePtr>(TYsonString(TUnbufferedFileInput(configFileName).ReadAll()));
    config.Load(node);

    RunTest(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

using namespace NYT;

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }
}
