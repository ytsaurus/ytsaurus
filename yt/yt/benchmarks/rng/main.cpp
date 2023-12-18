#include <yt/yt/core/profiling/timing.h>

#include <random>

template <typename TRng>
void BenchmarkRng(const TString& rngName)
{
    constexpr int Runs = 100'000'000;

    TRng rng;
    NYT::NProfiling::TWallTimer timer;

    for (int run = 0; run < Runs; ++run) {
        rng.seed(run);
        std::uniform_int_distribution<int>(0, 42)(rng);
    }

    Cerr << rngName << " " << timer.GetElapsedTime() << Endl;
}

int main()
{
    BenchmarkRng<std::mt19937>("mt19937");
    BenchmarkRng<std::mt19937_64>("mt19937_64");
    BenchmarkRng<std::minstd_rand>("minstd_rand");
    BenchmarkRng<std::minstd_rand0>("minstd_rand0");
}
