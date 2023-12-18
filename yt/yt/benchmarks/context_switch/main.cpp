#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/profiling/timing.h>

#include <atomic>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/execution_stack.h>
#include <yt/yt/core/concurrency/private.h>

#include <util/system/context.h>

using namespace NYT;
using namespace NConcurrency;
using namespace NProfiling;

YT_THREAD_LOCAL(TContMachineContext) CallerContext;

struct TMyFiber
    : public ITrampoLine
{

    TMyFiber()
        : Stack_(256_KB)
        , Context_({
            this,
            TArrayRef(static_cast<char*>(Stack_.GetStack()), Stack_.GetSize())})
    { }

    // ITrampoLine implementation
    void DoRunNaked() override
    {
        while (true) {
            ++SwitchCount;
            Context_.SwitchTo(&GetTlsRef(CallerContext));
        }
    }

    void SwitchInto()
    {
        ++SwitchCount;
        GetTlsRef(CallerContext).SwitchTo(&Context_);
    }

    size_t SwitchCount = 0;

    TExecutionStack Stack_;
    TContMachineContext Context_;

};

struct TBaseClass
{
    virtual void Foo() = 0;

    virtual ~TBaseClass() {}
};

struct TDerivedClass
    : public TBaseClass
{
    void Foo() override
    {
        ++CallCount;
    }

    size_t CallCount = 0;
};

Y_NO_INLINE void RunTest(TBaseClass* base, size_t iterations)
{
    for (size_t i = 0; i < iterations; ++i) {
        base->Foo();
    }
}

typedef void (*TRunTestCallBack)(TBaseClass* base, size_t iterations);

static TRunTestCallBack RunTestFn = &RunTest;

int main(int /*argc*/, char** /*argv*/)
{
    size_t iterations = 10000000;

    {
        TMyFiber fiber;

        auto startTime = NProfiling::GetCpuInstant();

        for (size_t i = 0; i < iterations ; ++i) {
            fiber.SwitchInto();
        }

        auto duration = NProfiling::GetCpuInstant() - startTime;

        Cout << Format("SwitchCount: %v\n", fiber.SwitchCount);

        Cout << Format("Context switch (Time: %v, Cycles/iter: %v, Cycles/switch: %v)\n",
            NProfiling::CpuDurationToDuration(duration),
            duration / iterations,
            duration / fiber.SwitchCount);
    }

    {
        std::atomic<int> value = 0;

        auto startTime = NProfiling::GetCpuInstant();
        for (size_t i = 0; i < iterations; ++i) {
            //value.exchange(i + 1);
            ++value;
        }
        auto duration = NProfiling::GetCpuInstant() - startTime;

        Cout << Format("Atomic add/exchange (Time: %v, Cycles/iter: %v)\n",
            NProfiling::CpuDurationToDuration(duration),
            duration / iterations);
    }

    {
        TDerivedClass object;

        auto runTest = RunTestFn;
        auto startTime = NProfiling::GetCpuInstant();

        runTest(&object, iterations);
        auto duration = NProfiling::GetCpuInstant() - startTime;

        Cout << Format("CallCount: %v\n", object.CallCount);

        Cout << Format("Virtual call (Time: %v, Cycles/iter: %v)\n",
            NProfiling::CpuDurationToDuration(duration),
            duration / iterations);
    }

    {
        auto startTime = NProfiling::GetCpuInstant();

        std::unique_ptr<int> ptr;

        for (size_t i = 0; i < iterations; ++i) {
            ptr = std::make_unique<int>();
        }
        auto duration = NProfiling::GetCpuInstant() - startTime;

        Cout << Format("Alloc/dealloc small (Time: %v, Cycles/iter: %v)\n",
            NProfiling::CpuDurationToDuration(duration),
            duration / iterations);
    }

    {
        auto startTime = NProfiling::GetCpuInstant();

        TCallback<void(size_t*)> closure;
        size_t callCount = 0;

        for (size_t i = 0; i < iterations; ++i) {
            closure = BIND([] (size_t* callCount) {
                ++*callCount;
            });

            closure(&callCount);
        }
        auto duration = NProfiling::GetCpuInstant() - startTime;

        Cout << Format("Callback create, call and destroy (Time: %v, Cycles/iter: %v)\n",
            NProfiling::CpuDurationToDuration(duration),
            duration / iterations);
    }

    return 0;
}
