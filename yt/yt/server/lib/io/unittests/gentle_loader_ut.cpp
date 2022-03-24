#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/gentle_loader.h>
#include <yt/yt/server/lib/io/io_workload_model.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NIO {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("GentleLoadTest");

////////////////////////////////////////////////////////////////////////////////

// Borrowed from ads/quality/ltp/libs/util/temp.h
class TTempDirectory
{
public:
    TTempDirectory()
    {
        auto tmpName = MakeTempName();
        NFs::Remove(tmpName);
        Name_ = tmpName;
        Y_ENSURE(NFs::MakeDirectory(Name_));
    }

    ~TTempDirectory()
    {
        NFs::RemoveRecursive(Name_);
    }

    const TString& GetName() const
    {
        return Name_;
    }

private:
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

struct TRandomFileProviderMock
    : public IRandomFileProvider
{
    virtual std::optional<TFileInfo> GetRandomExistingFile()
    {
        return TFileInfo{
            .Path = "fake_random_file",
            .DiskSpace = 1_GB,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIOEngineMockConfig
{
    int ThreadsCount = 4;
    TDuration OpenLatency;
    TDuration ReadLatency;
    TDuration WriteLatency;

    // Measured in percents.
    ui32 ReadFailingProbability = 0;
    ui32 WriteFailingProbability = 0;
    ui32 OpenFailingProbability = 0;

    TDuration UserRequestsP99;
};

////////////////////////////////////////////////////////////////////////////////

class TIOEngineMock
    : public IIOEngineWorkloadModel
{
public:
    explicit TIOEngineMock(const TIOEngineMockConfig& config)
        : Config_(config)
        , ThreadPool_(New<TThreadPool>(Config_.ThreadsCount, "MockWriter"))
        , Invoker_(ThreadPool_->GetInvoker())
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> /*requests*/,
        EWorkloadCategory /*category*/,
        TRefCountedTypeCookie /*tagCookie*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(Config_.ReadLatency, Config_.ReadFailingProbability)
            .Apply(BIND([] {
                return TReadResponse{};
            }));
    }

    TFuture<void> Write(
        TWriteRequest /*request*/,
        EWorkloadCategory /*category*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(Config_.WriteLatency, Config_.WriteFailingProbability);
    }

    TFuture<void> FlushFile(
        TFlushFileRequest /*request*/,
        EWorkloadCategory /*category*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest /*request*/,
        EWorkloadCategory /*category*/,
        TSessionId /*sessionId*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest /*request*/,
        EWorkloadCategory /*category*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest /*request*/,
        EWorkloadCategory /*category*/) override
    {
        return RunRequest(Config_.OpenLatency, Config_.OpenFailingProbability)
            .Apply(BIND([] {
                return New<TIOEngineHandle>();
            }));
    }

    TFuture<void> Close(
        TCloseRequest /*request*/,
        EWorkloadCategory /*category*/) override
    {
        return RunRequest(TDuration{});
    }

    TFuture<void> Allocate(
        TAllocateRequest /*request*/,
        EWorkloadCategory /*category*/) override
    {
        return RunRequest(TDuration{});
    }

    bool IsSick() const override
    {
        return false;
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<TRequestSizes> GetRequestSizes() override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<TRequestLatencies> GetRequestLatencies() override
    {
        TRequestLatencies latencies;
        auto& interatives = latencies.Reads[EWorkloadCategory::UserInteractive];
        interatives.RecordValue(Config_.UserRequestsP99.MilliSeconds(), 1000);
        latencies.Duration = TDuration::Seconds(5);
        return latencies;
    }

    i64 GetTotalReadBytes() const override
    {
        return 0;
    }

    i64 GetTotalWrittenBytes() const override
    {
        return 0;
    }

    TFuture<void> RunRequest(TDuration latency, ui32 failingProbability = 0)
    {
        return BIND([=, this_ = MakeStrong(this)] {
            Sleep(latency);
            if (failingProbability && RandomNumber<ui32>(100) < failingProbability) {
                THROW_ERROR_EXCEPTION("Mock request failed");
            }
        }).AsyncVia(Invoker_).Run();
    }

    void Reconfigure(const NYTree::INodePtr& config) override
    {
        Y_UNUSED(config);
    }

private:
    const TIOEngineMockConfig Config_;
    const TThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestRoundResult
{
    // Maximum IO per second when congested.
    i64 IOPS = 0;
};

class TGentleLoaderTest
    : public ::testing::Test
{
public:
    TGentleLoaderTest()
        : GentleLoaderConfig_(New<TGentleLoaderConfig>())
        , ActionQueue_(New<TActionQueue>("TestActionQueue"))
    {
        GentleLoaderConfig_->CongestionDetector->ProbeDeadline = TDuration::MilliSeconds(30);
        GentleLoaderConfig_->CongestionDetector->ProbesInterval = TDuration::MilliSeconds(30);
        GentleLoaderConfig_->WaitAfterCongested = TDuration::MilliSeconds(10);

        IOEngineConfig_ = TIOEngineMockConfig{
            .OpenLatency = TDuration::MilliSeconds(1),
            .ReadLatency = TDuration::MilliSeconds(10),
            .WriteLatency = TDuration::MilliSeconds(10),
        };
    }

protected:
    TTempDirectory LocationRoot_;
    TGentleLoaderConfigPtr GentleLoaderConfig_;
    TIOEngineMockConfig IOEngineConfig_;
    TActionQueuePtr ActionQueue_;

    std::vector<TTestRoundResult> Run(int roundsCount)
    {
        auto ioEngine = New<TIOEngineMock>(IOEngineConfig_);
        auto gentleLoader = CreateGentleLoader(
            GentleLoaderConfig_,
            LocationRoot_.GetName(),
            ioEngine,
            New<TRandomFileProviderMock>(),
            ActionQueue_->GetInvoker(),
            Logger);

        TNonblockingQueue<TTestRoundResult> queue;

        gentleLoader->SubscribeCongested(BIND([&] (i64 window) {
            queue.Enqueue(TTestRoundResult{
                .IOPS = window,
            });
        }));

        gentleLoader->Start();

        std::vector<TTestRoundResult> result;

        for (int index = 0; index < roundsCount; ++index) {
            auto roundResult = queue.Dequeue().Get()
                .ValueOrThrow();
            result.push_back(roundResult);
            YT_LOG_INFO("Next congested step (Index: %v, IOPS: %v)",
                index,
                roundResult.IOPS);
        }

        gentleLoader->Stop();

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TGentleLoaderTest, TestSimple)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // 4 threads with 100 IOPS each should get about 400 IOPS
    auto lastResult = results.back();
    EXPECT_TRUE((lastResult.IOPS > 350) && (lastResult.IOPS < 500));
}

TEST_F(TGentleLoaderTest, TestErrorHandling)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.OpenFailingProbability = 5;
    IOEngineConfig_.ReadFailingProbability = 5;
    IOEngineConfig_.WriteFailingProbability = 5;
    GentleLoaderConfig_->WritersCount = 20;
    GentleLoaderConfig_->CongestionDetector->OverloadThreshold = 50;

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // Check that we get some sane values even in case of errors.
    auto lastResult = results.back();
    EXPECT_TRUE((lastResult.IOPS > 200) && (lastResult.IOPS < 500));
}

TEST_F(TGentleLoaderTest, TestAllFailing)
{
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(10);
    IOEngineConfig_.ReadFailingProbability = 100;
    IOEngineConfig_.WriteFailingProbability = 100;

    GentleLoaderConfig_->SegmentSize = 1;

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);

    // Check that we some small IOPS before overload.
    auto lastResult = results.back();
    EXPECT_LE(lastResult.IOPS, 10);
}

TEST_F(TGentleLoaderTest, TestInteractiveOverloaded)
{
    // Setup high-rate environment.
    IOEngineConfig_.ThreadsCount = 4;
    IOEngineConfig_.ReadLatency = TDuration::MilliSeconds(1);
    IOEngineConfig_.WriteLatency = TDuration::MilliSeconds(1);

    GentleLoaderConfig_->SegmentSize = 1;

    // User interactive requests already overloaded.
    GentleLoaderConfig_->CongestionDetector->UserRequestOverloadThreshold = TDuration::MilliSeconds(100);
    IOEngineConfig_.UserRequestsP99 = TDuration::Seconds(1);

    auto results = Run(5);
    EXPECT_EQ(std::ssize(results), 5);
    // Check that we some small IOPS before overload.
    auto lastResult = results.back();
    EXPECT_LE(lastResult.IOPS, 10);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
