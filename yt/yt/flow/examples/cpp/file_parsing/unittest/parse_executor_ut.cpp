#include <yt/yt/flow/examples/cpp/file_parsing/lib/messages.h>
#include <yt/yt/flow/examples/cpp/file_parsing/lib/parse_executor.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NFileParsingExample {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr auto LongQueueWait = TDuration::Seconds(60);
constexpr auto ShortQueueWait = TDuration::MilliSeconds(100);
constexpr auto WaitTimeout = TDuration::Seconds(10);
//! How long a parse that must not start is given to start anyway.
constexpr auto AbsenceTimeout = TDuration::MilliSeconds(300);

////////////////////////////////////////////////////////////////////////////////

//! Runs #TParseExecutor with a parse that waits until the test releases it.
class TParseExecutorTest
    : public ::testing::Test
{
protected:
    const IThreadPoolPtr ThreadPool_ = CreateThreadPool(4, "ParseTest");

    void TearDown() override
    {
        ThreadPool_->Shutdown();
    }

    TParseExecutorPtr CreateExecutor(int slotCount, TDuration maxQueueWait)
    {
        auto config = New<TParseExecutorConfig>();
        config->SlotCount = slotCount;
        config->MaxQueueWait = maxQueueWait;
        return New<TParseExecutor>(config, ThreadPool_->GetInvoker(), BIND(&TParseExecutorTest::Parse, Unretained(this)));
    }

    static TParseTaskPtr MakeTask(const std::string& url, ui64 firstSeenAt)
    {
        auto task = New<TParseTask>();
        task->Url = url;
        task->Size = std::ssize(url);
        task->FirstSeenAt = firstSeenAt;
        return task;
    }

    //! Makes the wait of the parse of |url| ignore cancelation.
    void IgnoreCancelation(const std::string& url)
    {
        auto guard = Guard(Lock_);
        CancelationIgnoringUrls_.insert(url);
    }

    void WaitStarted(const std::string& url)
    {
        WaitFor(GetPromise(Starts_, url).ToFuture().ToUncancelable().WithTimeout(WaitTimeout))
            .ThrowOnError();
    }

    bool HasStarted(const std::string& url)
    {
        // A timeout cancels the future; the start promise must stay unset for a later start.
        return WaitFor(GetPromise(Starts_, url).ToFuture().ToUncancelable().WithTimeout(AbsenceTimeout)).IsOK();
    }

    void Release(const std::string& url)
    {
        GetPromise(Releases_, url).TrySet();
    }

    //! Returns how the parse of |url| was let go: released by the test or canceled.
    TError WaitReleased(const std::string& url)
    {
        return WaitFor(GetPromise(Releases_, url).ToFuture().ToUncancelable().WithTimeout(WaitTimeout));
    }

    std::vector<std::string> GetStartedUrls()
    {
        auto guard = Guard(Lock_);
        return StartedUrls_;
    }

    static TErrorOr<TParseResultPtr> WaitResult(const TFuture<TParseResultPtr>& future)
    {
        return WaitFor(future.WithTimeout(WaitTimeout));
    }

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::vector<std::string> StartedUrls_;
    THashSet<std::string> CancelationIgnoringUrls_;
    THashMap<std::string, TPromise<void>> Starts_;
    THashMap<std::string, TPromise<void>> Releases_;

    TPromise<void> GetPromise(THashMap<std::string, TPromise<void>>& promises, const std::string& url)
    {
        auto guard = Guard(Lock_);
        auto [it, inserted] = promises.emplace(url, TPromise<void>());
        if (inserted) {
            it->second = NewPromise<void>();
        }
        return it->second;
    }

    TParseResultPtr Parse(const TParseTaskPtr& task)
    {
        bool ignoreCancelation = false;
        {
            auto guard = Guard(Lock_);
            StartedUrls_.push_back(task->Url);
            ignoreCancelation = CancelationIgnoringUrls_.contains(task->Url);
        }
        GetPromise(Starts_, task->Url).TrySet();

        // The cancelation of the fiber cancels the awaited future.
        auto release = GetPromise(Releases_, task->Url).ToFuture();
        WaitFor(ignoreCancelation ? release.ToUncancelable() : release)
            .ThrowOnError();

        auto result = New<TParseResult>();
        result->RowCount = task->Size;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TParseExecutorTest, OldestFirst)
{
    auto executor = CreateExecutor(/*slotCount*/ 1, LongQueueWait);
    auto first = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto late = executor->Submit(MakeTask("b", 300));
    auto early = executor->Submit(MakeTask("cc", 200));
    auto middle = executor->Submit(MakeTask("ddd", 250));

    Release("a");
    WaitStarted("cc");
    Release("cc");
    WaitStarted("ddd");
    Release("ddd");
    WaitStarted("b");
    Release("b");

    EXPECT_EQ(WaitResult(first).ValueOrThrow()->RowCount, 1);
    EXPECT_EQ(WaitResult(early).ValueOrThrow()->RowCount, 2);
    EXPECT_EQ(WaitResult(middle).ValueOrThrow()->RowCount, 3);
    EXPECT_EQ(WaitResult(late).ValueOrThrow()->RowCount, 1);
    EXPECT_EQ(GetStartedUrls(), (std::vector<std::string>{"a", "cc", "ddd", "b"}));
}

TEST_F(TParseExecutorTest, SlotsRunInParallel)
{
    auto executor = CreateExecutor(/*slotCount*/ 2, LongQueueWait);
    auto first = executor->Submit(MakeTask("a", 100));
    auto second = executor->Submit(MakeTask("b", 200));
    auto third = executor->Submit(MakeTask("c", 300));
    WaitStarted("a");
    WaitStarted("b");
    EXPECT_FALSE(HasStarted("c"));

    Release("b");
    WaitStarted("c");
    Release("a");
    Release("c");
    EXPECT_TRUE(WaitResult(first).IsOK());
    EXPECT_TRUE(WaitResult(second).IsOK());
    EXPECT_TRUE(WaitResult(third).IsOK());
}

TEST_F(TParseExecutorTest, RejectsAfterMaxQueueWait)
{
    auto executor = CreateExecutor(/*slotCount*/ 1, ShortQueueWait);
    auto running = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto waiting = executor->Submit(MakeTask("b", 50));

    EXPECT_TRUE(WaitResult(waiting).ValueOrThrow()->Rejected);

    // The slot taken by the running parse is not given to the rejected task when it gets free.
    Release("a");
    EXPECT_TRUE(WaitResult(running).IsOK());
    auto next = executor->Submit(MakeTask("c", 200));
    WaitStarted("c");
    Release("c");
    EXPECT_TRUE(WaitResult(next).IsOK());
    EXPECT_EQ(GetStartedUrls(), (std::vector<std::string>{"a", "c"}));
}

TEST_F(TParseExecutorTest, TaskThatWaitedNeverStartsWithZeroQueueWait)
{
    // Whichever comes first, the rejection timer or the freed slot, rejects the waiting task.
    auto executor = CreateExecutor(/*slotCount*/ 1, TDuration::Zero());
    auto running = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto waiting = executor->Submit(MakeTask("b", 50));
    Release("a");

    EXPECT_TRUE(WaitResult(running).IsOK());
    EXPECT_TRUE(WaitResult(waiting).ValueOrThrow()->Rejected);
    EXPECT_EQ(GetStartedUrls(), (std::vector<std::string>{"a"}));
}

TEST_F(TParseExecutorTest, CancelWaiting)
{
    auto executor = CreateExecutor(/*slotCount*/ 1, LongQueueWait);
    auto running = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto canceled = executor->Submit(MakeTask("b", 200));
    auto next = executor->Submit(MakeTask("c", 300));

    canceled.Cancel(TError(NYT::EErrorCode::Canceled, "Test cancelation"));
    EXPECT_EQ(WaitResult(canceled).GetCode(), NYT::EErrorCode::Canceled);

    Release("a");
    WaitStarted("c");
    Release("c");
    EXPECT_TRUE(WaitResult(running).IsOK());
    EXPECT_TRUE(WaitResult(next).IsOK());
    EXPECT_EQ(GetStartedUrls(), (std::vector<std::string>{"a", "c"}));
}

TEST_F(TParseExecutorTest, CancelRunning)
{
    auto executor = CreateExecutor(/*slotCount*/ 1, LongQueueWait);
    auto canceled = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto next = executor->Submit(MakeTask("b", 200));

    // The parse stops at the cancelation and frees the slot.
    canceled.Cancel(TError(NYT::EErrorCode::Canceled, "Test cancelation"));
    EXPECT_EQ(WaitResult(canceled).GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_EQ(WaitReleased("a").GetCode(), NYT::EErrorCode::Canceled);
    WaitStarted("b");
    Release("b");
    EXPECT_TRUE(WaitResult(next).IsOK());
}

TEST_F(TParseExecutorTest, CanceledParseKeepsSlotUntilReturn)
{
    auto executor = CreateExecutor(/*slotCount*/ 1, LongQueueWait);
    IgnoreCancelation("a");
    auto canceled = executor->Submit(MakeTask("a", 100));
    WaitStarted("a");
    auto next = executor->Submit(MakeTask("b", 200));

    canceled.Cancel(TError(NYT::EErrorCode::Canceled, "Test cancelation"));
    EXPECT_FALSE(HasStarted("b"));

    // The wait of the parse ignores the cancelation, but the fiber resumes canceled, so the wait throws
    // and the task gets the cancelation error rather than the value.
    Release("a");
    EXPECT_EQ(WaitResult(canceled).GetCode(), NYT::EErrorCode::Canceled);
    WaitStarted("b");
    Release("b");
    EXPECT_TRUE(WaitResult(next).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NFileParsingExample
