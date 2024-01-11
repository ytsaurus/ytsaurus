#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

#include <library/cpp/yt/string/enum.h>

#include <yt/yt/core/misc/error.h>

#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMode,
    (None)
    (UseAfterFree)
);

////////////////////////////////////////////////////////////////////////////////

class TGwpAsanTest
    : public TProgram
{
public:
    TGwpAsanTest()
        : Generator_(/*seed*/ 42)
    {
        Opts_.AddLongOption("mode").StoreResult(&ModeString_);
        Opts_.AddLongOption("allocation-size").StoreResult(&AllocationSize_);
        Opts_.AddLongOption("guarded-sampling-rate").StoreResult(&GuardedSamplingRate_);
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        ConfigureCrashHandler();
        // We activate sampling rate manually in ConfigureSingletons.
        ConfigureAllocator({.TCMallocGuardedSamplingRate = std::nullopt});
        auto singletonsConfig = New<TSingletonsConfig>();
        singletonsConfig->TCMalloc->GuardedSamplingRate = GuardedSamplingRate_;
        ConfigureSingletons(singletonsConfig);

        constexpr i64 MaxSanitizedAllocationSize = 256_KB;

        if (AllocationSize_ > MaxSanitizedAllocationSize) {
            THROW_ERROR_EXCEPTION("GWP-ASan does not sample allocations larger than 256 KiB");
        }

        Cerr << "Heating up" << Endl;
        HeatUp();
        Cerr << "Ready" << Endl;

        try {
            Mode_ = ParseEnum<EMode>(ModeString_);
        } catch (const std::exception& ex) {
            auto domainValues = TEnumTraits<EMode>::GetDomainValues();
            THROW_ERROR_EXCEPTION(
                "Unknown mode %Qv, expected one of %lv",
                ModeString_,
                std::vector(domainValues.begin(), domainValues.end()))
                << TError(ex);
        }

        i64 cumulativeAllocationSize = 0;
        i64 reportPeriodBytes = 100_MB;
        i64 previousPeriod = -1;
        i64 previousCumulativeAllocationSize = 0;
        TInstant previousInstant;

        double slidingAverage = 0;

        for (ui64 iteration = 0; ; ++iteration) {
            cumulativeAllocationSize += AllocationSize_;
            auto currentPeriod = cumulativeAllocationSize / reportPeriodBytes;
            if (currentPeriod != previousPeriod) {
                auto now = TInstant::Now();
                auto duration = now - previousInstant;
                double bytes = cumulativeAllocationSize - previousCumulativeAllocationSize;
                std::optional<double> throughput = previousPeriod >= 0
                    ? std::make_optional(static_cast<double>(bytes) / 1_MB / duration.SecondsFloat())
                    : std::nullopt;
                Cerr
                    << Format(
                        "Iteration: %v, Allocated: %v MiB, Throughput: %.2v MiB/sec, SlidingAvgThroughput: %.0v MiB/sec",
                        iteration,
                        cumulativeAllocationSize / 1_MB,
                        throughput,
                        slidingAverage)
                    << Endl;
                if (throughput) {
                    if (slidingAverage == 0) {
                        slidingAverage = *throughput;
                    } else {
                        slidingAverage = slidingAverage * 0.99 + *throughput * 0.01;
                    }
                }
                previousPeriod = currentPeriod;
                previousCumulativeAllocationSize = cumulativeAllocationSize;
                previousInstant = now;
            }

            if (iteration == std::numeric_limits<i64>::max()) {
                Cerr << "How did you get here? " << DoNotOptimizeMePls_ << Endl;
                return;
            }

            RunTest();
        }
    }

    void HeatUp()
    {
        // Despite our configuration, first guarded allocation will happen around first ~100 allocations.
        // due to tcmalloc implementation. In order to overcome that, we allocate around 1 GiB by portions of 8 KiB on start.
        constexpr int HeatAllocationCount = 1_GB / 8_KB;
        for (int allocationIndex = 0; allocationIndex < HeatAllocationCount; ++allocationIndex) {
            std::vector<char> allocation;
            FillAllocation(allocation, 8_KB);
        }
    }

    void FillAllocation(std::vector<char>& allocation, i64 size)
    {
        allocation.resize(size + ((DoNotOptimizeMePls_ == 42) ? 1 : 0));
        std::memset(allocation.data(), 0xfu, size);
        DoNotOptimizeMePls_ += reinterpret_cast<intptr_t>(&allocation.back());
    }

    void RunTest()
    {
        switch (Mode_) {
            case EMode::None: {
                std::vector<char> allocation;
                FillAllocation(allocation, AllocationSize_);
                break;
            }
            case EMode::UseAfterFree: {
                volatile char* ptr;
                {
                    std::vector<char> allocation;
                    FillAllocation(allocation, AllocationSize_);
                    auto index = std::uniform_int_distribution<int>(0, AllocationSize_ - 1)(Generator_);
                    ptr = &allocation[index];
                    DoNotOptimizeMePls_ += reinterpret_cast<intptr_t>(allocation.data());
                }
                *ptr = 42;
                break;
            }
        }
    }

private:
    TString ModeString_ = "none";
    EMode Mode_;
    i64 AllocationSize_ = 8_KB;
    i64 GuardedSamplingRate_ = 128_MB;
    std::mt19937 Generator_;

    ui64 DoNotOptimizeMePls_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TGwpAsanTest().Run(argc, argv);
}
