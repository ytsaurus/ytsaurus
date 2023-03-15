#include <yt/cpp/roren/bigrt/execution_block.h>
#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/bigrt_executor.h>
#include <yt/cpp/roren/bigrt/bigrt_execution_context.h>
#include <yt/cpp/roren/interface/private/raw_par_do.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <functional>

using namespace NRoren;
using namespace NRoren::NPrivate;
using namespace NYT;
using namespace NYT::NConcurrency;

template <typename T>
TSharedRef ToSharedRef(const std::initializer_list<T>& data)
{
    return TSharedRef::MakeCopy<TDefaultSharedBlobTag>(TRef(std::data(data), std::data(data) + data.size()));
}

template <typename T>
TSharedRef ToSharedRef(const std::vector<T>& data)
{
    TSharedRef::MakeCopy<TDefaultSharedBlobTag>(TRef(data.data(), data.data() + data.size()));
}

//
// SERIALIZED
//

class TSerializedExecutionBlockTest
    : public testing::Test
{
public:
    const TTypeTag<int> ResultTag = TTypeTag<int>{"result"};
    NYT::NConcurrency::IThreadPoolPtr ThreadPool = NYT::NConcurrency::CreateThreadPool(1, "test-thread-pool");
    TBigRtMemoryStorage Storage;
    IRawOutputPtr RawOutput = Storage.AddOutput(ResultTag);

public:
    void TearDown() override
    {
        ThreadPool->Shutdown();
    }

    std::vector<int> GetResult()
    {
        return Storage.ToResult()->GetRowList(ResultTag);
    }
};

TEST_F(TSerializedExecutionBlockTest, Simple)
{
    IRawParDoPtr parDo = MakeRawParDo<int, int>([] (const int& input) -> int {
        return input + input;
    });

    auto executionBlock = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        parDo,
        std::vector<IRawOutputPtr>{RawOutput}
    );

    executionBlock->BindToInvoker(ThreadPool->GetInvoker());

    auto finished = executionBlock->Start(DummyExecutionContext());
    executionBlock->AsyncDo(ToSharedRef({1, 2, 3}));
    executionBlock->AsyncFinish();

    WaitFor(finished)
        .ThrowOnError();

    auto expected = std::vector{2, 4, 6};
    EXPECT_EQ(GetResult(), expected);
}

TEST_F(TSerializedExecutionBlockTest, Pipe)
{
    IRawParDoPtr parDo2 = MakeRawParDo<int, int>([] (const int& input) -> int {
        return input + input;
    });
    auto executionBlock2 = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        parDo2,
        std::vector<IRawOutputPtr>{RawOutput}
    );

    IRawParDoPtr parDo1 = MakeRawParDo<int, int>([] (const int& input) -> int {
        return input + 2;
    });
    auto executionBlock1 = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        parDo1,
        std::vector<IExecutionBlockPtr>{executionBlock2}
    );

    executionBlock1->BindToInvoker(ThreadPool->GetInvoker());

    auto finished = executionBlock1->Start(DummyExecutionContext());
    executionBlock1->AsyncDo(ToSharedRef({1, 2, 3}));
    executionBlock1->AsyncFinish();

    WaitFor(finished)
        .ThrowOnError();

    auto expected = std::vector{6, 8, 10};
    EXPECT_EQ(GetResult(), expected);
}

TEST_F(TSerializedExecutionBlockTest, Exception)
{
    IRawParDoPtr parDo = MakeRawParDo<int, int>([] (const int&) -> int {
        ythrow yexception() << "oops";
    });

    auto executionBlock = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        parDo,
        std::vector<IRawOutputPtr>{RawOutput}
    );

    executionBlock->BindToInvoker(ThreadPool->GetInvoker());

    auto finished = executionBlock->Start(DummyExecutionContext());
    executionBlock->AsyncDo(ToSharedRef({1, 2, 3}));
    executionBlock->AsyncFinish();

    auto result = WaitFor(finished);
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TSerializedExecutionBlockTest, StartOrder)
{
    class TCheckStartedDoFn : public IDoFn<int, int>
    {
    public:
        void Start(TOutput<int>&) override
        {
            Started_ = true;
        }

        void Do(const int& x, TOutput<int>& output) override
        {
            if (!Started_) {
                ythrow yexception() << "ParDo was not started";
            }
            output.Add(x + x);
        }

    private:
        bool Started_ = false;
    };

    class TWriteInStartDoFn : public IDoFn<int, int>
    {
    public:
        void Start(TOutput<int>& output) override
        {
            output.Add(42);
        }

        void Do(const int& x, TOutput<int>& output) override
        {
            output.Add(x);
        }
    };

    auto innerParDo = MakeRawParDo(::MakeIntrusive<TCheckStartedDoFn>());
    auto innerBlock = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        innerParDo,
        std::vector{RawOutput}
    );

    auto outerParDo(MakeRawParDo(::MakeIntrusive<TWriteInStartDoFn>()));
    auto outerBlock = CreateSerializedExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        outerParDo,
        std::vector{innerBlock}
    );

    outerBlock->BindToInvoker(ThreadPool->GetInvoker());

    auto finished = outerBlock->Start(DummyExecutionContext());
    outerBlock->AsyncDo(ToSharedRef({1, 2, 3}));
    outerBlock->AsyncFinish();

    WaitFor(finished)
        .ThrowOnError();

    const auto expected = std::vector({84, 2, 4, 6});
    ASSERT_EQ(GetResult(), expected);
}

//
// CONCURRENT
//

class TConcurrentExecutionBlockTest
    : public testing::Test
{
public:
    const TTypeTag<int> ResultTag = TTypeTag<int>{"result"};
    NYT::NConcurrency::IThreadPoolPtr ThreadPool = NYT::NConcurrency::CreateThreadPool(3, "test-thread-pool");
    TBigRtMemoryStorage Storage;
    IRawOutputPtr RawOutput = Storage.AddOutput(ResultTag);

public:
    void TearDown() override
    {
        ThreadPool->Shutdown();
    }

    std::vector<int> GetResult()
    {
        return Storage.ToResult()->GetRowList(ResultTag);
    }
};

TEST_F(TConcurrentExecutionBlockTest, Simple)
{
    IRawParDoPtr parDo = MakeRawParDo<int, int>([] (const int& input) -> int {
        // Long computations...
        Sleep(TDuration::MilliSeconds(5));
        return input + input;
    });

    auto executionBlock = CreateConcurrentExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        parDo,
        std::vector<IExecutionBlockPtr>{
            CreateOutputExecutionBlock({.DesiredInputElementCount = 10}, RawOutput, MakeRowVtable<int>()),
        }
    );

    executionBlock->BindToInvoker(ThreadPool->GetInvoker());

    auto finished = executionBlock->Start(DummyExecutionContext());

    executionBlock->AsyncDo(ToSharedRef({1, 2, 3}));
    executionBlock->AsyncDo(ToSharedRef({4, 5, 6}));
    executionBlock->AsyncDo(ToSharedRef({7, 8, 9}));
    executionBlock->AsyncDo(ToSharedRef({10, 11, 12}));
    executionBlock->AsyncDo(ToSharedRef({13, 14, 15}));

    executionBlock->AsyncFinish();

    WaitFor(finished)
        .ThrowOnError();

    auto actual = GetResult();
    std::sort(actual.begin(), actual.end());

    const auto expected = std::vector{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
    EXPECT_EQ(actual, expected);
}

class TMetricCounter : public IDoFn<int, int>
{
public:
    void Start(TOutput<int>& ) override
    {
        Counter_ = 0;
    }

    void Do(const int& , TOutput<int>&) override
    {
        // Racy increment;
        // Such increment will break if we try to use our class from different threads.
        Tmp_ = Counter_;
        Sleep(TDuration::MilliSeconds(5));
        ++Tmp_;
        Counter_ = Tmp_;
    }

    void Finish(TOutput<int>& output) override
    {
        output.Add(Counter_);
    }

private:
    int Counter_ = 0;
    int Tmp_ = 0;
};

TEST_F(TConcurrentExecutionBlockTest, TestDifferentDoFns)
{

    auto executionBlock = CreateConcurrentExecutionBlock(
        TBlockPipeConfig{.DesiredInputElementCount = 10},
        MakeRawParDo(::MakeIntrusive<TMetricCounter>()),
        std::vector<IExecutionBlockPtr>{
            CreateOutputExecutionBlock({.DesiredInputElementCount = 10}, RawOutput, MakeRowVtable<int>()),
        }
    );

    executionBlock->BindToInvoker(ThreadPool->GetInvoker());
    auto finished = executionBlock->Start(DummyExecutionContext());

    for (int i = 0; i != 100; ++i) {
        executionBlock->AsyncDo(ToSharedRef({1, 2, 3}));
    }

    executionBlock->AsyncFinish();
    WaitFor(finished)
        .ThrowOnError();

    auto actual = GetResult();
    int sum = 0;
    for (auto& v : actual) {
        sum += v;
    }

    EXPECT_EQ(sum, 300) << "Unequal: " << testing::PrintToString(actual);
}
