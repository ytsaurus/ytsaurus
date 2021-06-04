#include "util/datetime/base.h"
#include "util/stream/fwd.h"
#include <dlfcn.h>
#include <gtest/gtest.h>

#include <util/string/cast.h>
#include <util/stream/file.h>

#include <library/cpp/testing/common/env.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <size_t Index>
Y_NO_INLINE void BurnCpu()
{
    THash<TString> hasher;
    ui64 value = 0;
    for (int i = 0; i < 10000000; i++) {
        value += hasher(ToString(i));
    }
    EXPECT_NE(Index, value);
}

static std::atomic<int> Counter{0};

struct TNoTailCall
{
    ~TNoTailCall()
    {
        Counter++;
    }
};

static Y_NO_INLINE void StaticFunction()
{
    TNoTailCall noTail;
    BurnCpu<0>();
}

void RunUnderProfiler(const TString& name, std::function<void()> work, bool checkSamples = true)
{
    TCpuProfilerOptions options;
    options.SamplingFrequency = 100000;

    TCpuProfiler profiler(options);

    profiler.Start();

    work();

    profiler.Stop();

    auto profile = profiler.ReadProfile();
    if (checkSamples) {
        ASSERT_NE(0, profile.sample_size());
    }

    Symbolize(&profile);

    TFileOutput output(GetOutputPath() / name);
    WriteProfile(&output, profile);
    output.Finish();
}

TEST(TCpuProfiler, SingleThreadRun)
{
    RunUnderProfiler("single_thread.pb.gz", [] {
        BurnCpu<0>();
    });
}

TEST(TCpuProfiler, MultipleThreads)
{
    RunUnderProfiler("multiple_threads.pb.gz", [] {
        std::thread t1([] {
            BurnCpu<1>();
        });

        std::thread t2([] {
            BurnCpu<2>();
        });

        t1.join();
        t2.join();
    });
}

TEST(TCpuProfiler, StaticFunction)
{
    RunUnderProfiler("static_function.pb.gz", [] {
        StaticFunction();
    });
}

Y_NO_INLINE void RecursiveFunction(int n)
{
    TNoTailCall noTail;
    if (n == 0) {
        BurnCpu<0>();
    } else {
        RecursiveFunction(n-1);
    }
}

TEST(TCpuProfiler, DeepRecursion)
{
    RunUnderProfiler("recursive_function.pb.gz", [] {
        RecursiveFunction(1024);
    });
}

TEST(TCpuProfiler, DlOpen)
{
    RunUnderProfiler("dlopen.pb.gz", [] {
        auto libraryPath = BinaryPath("yt/yt/library/ytprof/unittests/testso/libtestso.so");

        auto dl = dlopen(libraryPath.c_str(), RTLD_LAZY);
        ASSERT_TRUE(dl);

        auto sym = dlsym(dl, "CallNext");
        ASSERT_TRUE(sym);

        auto callNext = reinterpret_cast<void(*)(void(*)())>(sym);
        callNext(BurnCpu<0>);
    });
}

TEST(TCpuProfiler, DlClose)
{
    RunUnderProfiler("dlclose.pb.gz", [] {
        auto libraryPath = BinaryPath("yt/yt/library/ytprof/unittests/testso1/libtestso1.so");

        auto dl = dlopen(libraryPath.c_str(), RTLD_LAZY);
        ASSERT_TRUE(dl);

        auto sym = dlsym(dl, "CallOtherNext");
        ASSERT_TRUE(sym);

        auto callNext = reinterpret_cast<void(*)(void(*)())>(sym);
        callNext(BurnCpu<0>);

        ASSERT_EQ(dlclose(dl), 0);
    });
}

void ReadUrandom()
{
    TIFStream input("/dev/urandom");

    std::array<char, 1 << 20> buffer;

    for (int i = 0; i < 100; i++) {
        input.Read(buffer.data(), buffer.size());
    }
}

TEST(TCpuProfiler, Syscalls)
{
    RunUnderProfiler("syscalls.pb.gz", [] {
        ReadUrandom();
    });
}

TEST(TCpuProfiler, VDSO)
{
    RunUnderProfiler("vdso.pb.gz", [] {
        auto now = TInstant::Now();
        while (TInstant::Now() < now + TDuration::MilliSeconds(100))
        { }
    }, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
