#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/memory_tag.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fiber.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler.h>

#include <util/random/random.h>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Used for fake side effects to disable compiler optimizations.
volatile const void* FakeSideEffectVolatileVariable = nullptr;

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NConcurrency;
using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagTest
    : public TestWithParam<void(*)()>
{
public:
    TMemoryTagTest() = default;
};

////////////////////////////////////////////////////////////////////////////////

// Allocate vector that results in exactly `size` memory usage considering the 16-byte header.
std::vector<char> MakeAllocation(size_t size)
{
    YCHECK(size > 16);
    // Size should be a power of two:
    YCHECK((size & (size - 1)) == 0);
<<<<<<< HEAD
    auto result = std::vector<char>(size - 16);

    // We make fake side effect to prevent any compiler optimizations here.
    // (Clever compilers like to throw away our unused allocations).
    FakeSideEffectVolatileVariable = result.data();
    return result;
=======
    // Do not forget about 16-byte tag header.
    return std::vector<char>(size - 16);
>>>>>>> Working on YTAlloc
}

// GDB does not allow to set breakpoints inside lambda functions, so
// I prefer using global functions.

void TestSimple()
{
    for (int allocationSize = 1 << 5; allocationSize <= (1 << 20); allocationSize <<= 1) {
        {
            TMemoryTagGuard guard(42);
            auto allocation = MakeAllocation(allocationSize);
            EXPECT_EQ(GetMemoryUsageForTag(42), allocationSize);
        }
        EXPECT_EQ(GetMemoryUsageForTag(42), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TestStackingGuards()
{
    TMemoryTagGuard guard1(1);
    EXPECT_EQ(GetMemoryUsageForTag(1), 0);
    auto allocation1 = MakeAllocation(1 << 5);
    EXPECT_EQ(GetMemoryUsageForTag(1), 1 << 5);
    {
        TMemoryTagGuard guard2(2);
        auto allocation2 = MakeAllocation(1 << 6);
        EXPECT_EQ(GetMemoryUsageForTag(1), 1 << 5);
        EXPECT_EQ(GetMemoryUsageForTag(2), 1 << 6);
    }
    EXPECT_EQ(GetMemoryUsageForTag(1), 1 << 5);
    EXPECT_EQ(GetMemoryUsageForTag(2), 0);
    {
        TMemoryTagGuard guard2(std::move(guard1));
        auto allocation2 = MakeAllocation(1 << 7);
        EXPECT_EQ(GetMemoryUsageForTag(1), (1 << 5) + (1 << 7));
        EXPECT_EQ(GetMemoryUsageForTag(2), 0);
    }
    EXPECT_EQ(GetMemoryUsageForTag(1), (1 << 5));
    EXPECT_EQ(GetMemoryUsageForTag(2), 0);
}

////////////////////////////////////////////////////////////////////////////////

void Action1()
{
    TMemoryTagGuard guard(1);
    Yield();
    auto allocation1 = MakeAllocation(1 << 5);
    EXPECT_EQ(GetMemoryUsageForTag(1), 1 << 5);
    Yield();
    auto allocation2 = MakeAllocation(1 << 7);
    EXPECT_EQ(GetMemoryUsageForTag(1), (1 << 5) + (1 << 7));
    Yield();
    auto allocation3 = MakeAllocation(1 << 9);
    EXPECT_EQ(GetMemoryUsageForTag(1), (1 << 5) + (1 << 7) + (1 << 9));
}

void Action2()
{
    TMemoryTagGuard guard(2);
    Yield();
    auto allocation1 = MakeAllocation(1 << 6);
    EXPECT_EQ(GetMemoryUsageForTag(2), 1 << 6);
    Yield();
    auto allocation2 = MakeAllocation(1 << 8);
    EXPECT_EQ(GetMemoryUsageForTag(2), (1 << 6) + (1 << 8));
    Yield();
    auto allocation3 = MakeAllocation(1 << 10);
    EXPECT_EQ(GetMemoryUsageForTag(2), (1 << 6) + (1 << 8) + (1 << 10));
}

void TestSwitchingFibers()
{
    auto future1 = BIND(&Action1)
        .AsyncVia(GetCurrentInvoker())
        .Run();
    auto future2 = BIND(&Action2)
        .AsyncVia(GetCurrentInvoker())
        .Run();
    WaitFor(Combine(std::vector<TFuture<void>>{future1, future2}))
        .ThrowOnError();
    EXPECT_EQ(GetMemoryUsageForTag(1), 0);
    EXPECT_EQ(GetMemoryUsageForTag(2), 0);
}

////////////////////////////////////////////////////////////////////////////////

class TMiniController
    : public TRefCounted
{
public:
    TMiniController(IInvokerPtr controlInvoker, TMemoryTag memoryTag)
        : MemoryTag_(memoryTag)
        , Invoker_(CreateMemoryTaggingInvoker(CreateSerializedInvoker(std::move(controlInvoker)), MemoryTag_))
    { }

    ssize_t GetMemoryUsage() const
    {
        return GetMemoryUsageForTag(MemoryTag_);
    }

    IInvokerPtr GetControlInvoker() const
    {
        return Invoker_;
    }

    std::vector<std::vector<char>>& Allocations()
    {
        return Allocations_;
    }

private:
    TMemoryTag MemoryTag_;
    IInvokerPtr Invoker_;
    std::vector<std::vector<char>> Allocations_;
};

DEFINE_REFCOUNTED_TYPE(TMiniController)
DECLARE_REFCOUNTED_CLASS(TMiniController)

void Action3(TMiniControllerPtr controller)
{
    controller->Allocations().emplace_back(MakeAllocation(128_MB));
}

void TestMemoryTaggingInvoker()
{
    auto queue = New<TActionQueue>();
    auto controller = New<TMiniController>(queue->GetInvoker(), 1);
    EXPECT_EQ(controller->GetMemoryUsage(), 0);

    WaitFor(BIND(&Action3, controller)
        .AsyncVia(controller->GetControlInvoker())
        .Run())
        .ThrowOnError();
    EXPECT_NEAR(controller->GetMemoryUsage(), 128_MB, 1_MB);

    controller->Allocations().clear();
    controller->Allocations().shrink_to_fit();

    EXPECT_NEAR(GetMemoryUsageForTag(1), 0, 1_MB);
}

void TestControllersInThreadPool()
{
    std::vector<TMiniControllerPtr> controllers;
    constexpr int controllerCount = 1000;
    auto pool = New<TThreadPool>(16, "TestPool");
    for (int index = 0; index < controllerCount; ++index) {
        controllers.emplace_back(New<TMiniController>(pool->GetInvoker(), index + 1));
    }
    constexpr int actionCount = 100 * 1000;
    std::vector<TFuture<void>> futures;
    std::vector<int> memoryUsages(controllerCount);
    srand(42);
    for (int index = 0; index < actionCount; ++index) {
        int controllerIndex = rand() % controllerCount;
        auto allocationSize = 1 << (5 + rand() % 10);
        memoryUsages[controllerIndex] += allocationSize;
        const auto& controller = controllers[controllerIndex];
        futures.emplace_back(
            BIND([] (TMiniControllerPtr controller, int allocationSize) {
                controller->Allocations().emplace_back(MakeAllocation(allocationSize));
            }, controller, allocationSize)
                .AsyncVia(controller->GetControlInvoker())
                .Run());
    }
    WaitFor(Combine(futures))
        .ThrowOnError();
    for (int index = 0; index < controllerCount; ++index) {
        EXPECT_NEAR(memoryUsages[index], controllers[index]->GetMemoryUsage(), 10_KB);
    }
    controllers.clear();
    for (int index = 0; index < controllerCount; ++index) {
        EXPECT_NEAR(GetMemoryUsageForTag(index + 1), 0, 10_KB);
        EXPECT_GE(GetMemoryUsageForTag(index + 1), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

#ifndef _asan_enabled_

TEST_P(TMemoryTagTest, Test)
{
    // We wrap anything with an outer action queue to make
    // fiber-friendly environment.
    auto outerQueue = New<TActionQueue>();
    WaitFor(BIND(GetParam())
        .AsyncVia(outerQueue->GetInvoker())
        .Run())
        .ThrowOnError();
}

INSTANTIATE_TEST_CASE_P(MemoryTagTest, TMemoryTagTest, Values(
    &TestSimple,
    &TestStackingGuards,
    &TestSwitchingFibers,
    &TestMemoryTaggingInvoker,
    &TestControllersInThreadPool));

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

