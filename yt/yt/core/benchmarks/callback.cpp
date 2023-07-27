#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>

#include <benchmark/benchmark.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void DoNotOptimizeAway(T&& datum) {
    asm volatile("" : "+r" (datum));
}

#define FORCE_OUT_OF_LINE __attribute__((noinline))
#define FORCE_SIDE_EFFECT do { int x = 1; DoNotOptimizeAway(x); asm volatile(""); } while(0)

////////////////////////////////////////////////////////////////////////////////

FORCE_OUT_OF_LINE void RegularFunction()
{
    FORCE_SIDE_EFFECT;
}

void Callback_RegularFunction(benchmark::State& state)
{
    while (state.KeepRunning()) {
        RegularFunction();
    }
}

BENCHMARK(Callback_RegularFunction);

void Callback_RegularCallback(benchmark::State& state)
{
    auto callback = BIND(&RegularFunction);
    while (state.KeepRunning()) {
        callback();
    }
}

BENCHMARK(Callback_RegularCallback);

////////////////////////////////////////////////////////////////////////////////

void Callback_LambdaFunction(benchmark::State& state)
{
    auto fn = [] () { FORCE_SIDE_EFFECT; };
    while (state.KeepRunning()) {
        fn();
    }
}

BENCHMARK(Callback_LambdaFunction);

void Callback_LambdaCallback(benchmark::State& state)
{
    auto fn = [] () { FORCE_SIDE_EFFECT; };
    auto callback = BIND(std::move(fn));
    while (state.KeepRunning()) {
        callback();
    }
}

BENCHMARK(Callback_LambdaCallback);

////////////////////////////////////////////////////////////////////////////////

class BaseClass
{
public:
    virtual ~BaseClass() {};
    virtual void VirtualFunction() = 0;
};

class DerivedClass
    : public BaseClass
{
public:
    virtual ~DerivedClass() {};

    FORCE_OUT_OF_LINE void MemberFunction()
    {
        FORCE_SIDE_EFFECT;
    }

    FORCE_OUT_OF_LINE void VirtualFunction() override
    {
        FORCE_SIDE_EFFECT;
    }
};

DerivedClass Instance;
DerivedClass* InstancePtr = &Instance;

void Callback_MemberFunction(benchmark::State& state)
{
    while (state.KeepRunning()) {
        Instance.MemberFunction();
    }
}

BENCHMARK(Callback_MemberFunction);

void Callback_MemberCallback(benchmark::State& state)
{
    auto callback = BIND(&DerivedClass::MemberFunction, InstancePtr);
    while (state.KeepRunning()) {
        callback();
    }
}

BENCHMARK(Callback_MemberCallback);

void Callback_VirtualFunction(benchmark::State& state)
{
    while (state.KeepRunning()) {
        Instance.VirtualFunction();
    }
}

BENCHMARK(Callback_VirtualFunction);

void Callback_VirtualCallback(benchmark::State& state)
{
    auto callback = BIND(&DerivedClass::VirtualFunction, InstancePtr);
    while (state.KeepRunning()) {
        callback();
    }
}

BENCHMARK(Callback_VirtualCallback);

////////////////////////////////////////////////////////////////////////////////

// Yet another simple benchmarks.
FORCE_OUT_OF_LINE int Sum(int a, int b, int c, int d, int e, int f)
{
    return a + b + c + d + e + f;
}

void Callback_SumF(benchmark::State& state)
{
    while (state.KeepRunning()) {
        int x = Sum(1, 2, 3, 4, 5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum0(benchmark::State& state)
{
    auto callback = BIND(&Sum);
    while (state.KeepRunning()) {
        int x = callback(1, 2, 3, 4, 5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum1(benchmark::State& state)
{
    auto callback = BIND(BIND(&Sum), 1);
    while (state.KeepRunning()) {
        int x = callback(2, 3, 4, 5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum2(benchmark::State& state)
{
    auto callback = BIND(BIND(BIND(&Sum), 1), 2);
    while (state.KeepRunning()) {
        int x = callback(3, 4, 5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum3(benchmark::State& state)
{
    auto callback = BIND(BIND(BIND(BIND(&Sum), 1), 2), 3);
    while (state.KeepRunning()) {
        int x = callback(4, 5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum4(benchmark::State& state)
{
    auto callback = BIND(BIND(BIND(BIND(BIND(&Sum), 1), 2), 3), 4);
    while (state.KeepRunning()) {
        int x = callback(5, 6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum5(benchmark::State& state)
{
    auto callback = BIND(BIND(BIND(BIND(BIND(BIND(&Sum), 1), 2), 3), 4), 5);
    while (state.KeepRunning()) {
        int x = callback(6);
        DoNotOptimizeAway(x);
    }
}

void Callback_Sum6(benchmark::State& state)
{
    auto callback = BIND(BIND(BIND(BIND(BIND(BIND(BIND(&Sum), 1), 2), 3), 4), 5), 6);
    while (state.KeepRunning()) {
        int x = callback();
        DoNotOptimizeAway(x);
    }
}

BENCHMARK(Callback_SumF);
BENCHMARK(Callback_Sum0);
BENCHMARK(Callback_Sum1);
BENCHMARK(Callback_Sum2);
BENCHMARK(Callback_Sum3);
BENCHMARK(Callback_Sum4);
BENCHMARK(Callback_Sum5);
BENCHMARK(Callback_Sum6);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
