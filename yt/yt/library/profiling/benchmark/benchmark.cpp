#include <yt/yt/library/profiling/solomon/cube.h>
#include <yt/yt/library/profiling/solomon/registry.h>
#include <yt/yt/library/profiling/tag.h>

#include <util/stream/buffer.h>

#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/monlib/metrics/metric_type.h>

#include <benchmark/benchmark.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

void BM_CounterRegistration(benchmark::State& state)
{
    auto impl = New<TSolomonRegistry>();
    TProfiler profiler(impl, "/debug");

    for (auto _ : state) {
        profiler.Counter("/counter");
    }
}

BENCHMARK(BM_CounterRegistration);

////////////////////////////////////////////////////////////////////////////////

constexpr int NumMethods = 20;
constexpr int NumUsers = 100;
constexpr int NumErrors = 30;

static_assert(NumMethods * NumUsers * NumErrors == 60000);

void BM_CounterCube(benchmark::State& state)
{
    auto tag = [&] (int method, int user, int error) -> TTagIdList {
        return TTagIdList{method, user + NumMethods, error + NumMethods + NumUsers};
    };

    TCube<i64> cube{12, 0};

    TTagIndexList emptyList{NoTagSentinel, NoTagSentinel, NoTagSentinel};

    auto rangeAll = [&] (auto fn) {
        for (int user = 0; user < NumUsers; ++user) {
            for (int method = 0; method < NumMethods; ++method) {
                for (int error = 0; error < NumErrors; ++error) {
                    RangeSubsets(tag(user, method, error), emptyList, emptyList, {}, {}, emptyList, fn);
                }
            }
        }
    };

    rangeAll([&] (auto tags) {
        cube.Add(std::move(tags));
    });

    for (auto _ : state) {
        cube.StartIteration();
        rangeAll([&] (auto tags) {
            cube.Update(std::move(tags), 1);
        });
        cube.FinishIteration();
    }
}

BENCHMARK(BM_CounterCube)
    ->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

auto RpcWorkload(TProfiler profiler)
{
    std::vector<TCounter> counters;
    for (int user = 0; user < NumUsers; ++user) {
        for (int method = 0; method < NumMethods; ++method) {
            for (int error = 0; error < NumErrors; ++error) {
                counters.push_back(profiler
                    .WithSparse()
                    .WithTag("user", ToString(user))
                    .WithTag("method", ToString(method))
                    .WithTag("error", ToString(error))
                    .Counter("/error_count"));
            }
        }
    }
    return counters;
}

////////////////////////////////////////////////////////////////////////////////

auto MethodLatencyWorkload(TProfiler profiler)
{
    std::vector<TEventTimer> counters;
    for (int method = 0; method < NumMethods; ++method) {
        counters.push_back(profiler
            .WithTag("service", "MyService")
            .WithTag("method", ToString(method), -1)
            .Histogram("/lookup_latency", TDuration::MilliSeconds(1), TDuration::Seconds(1)));
    }
    return counters;
}

////////////////////////////////////////////////////////////////////////////////

auto SchedulerWorkload(TProfiler r)
{
    std::vector<TGauge> gauges;
    std::vector<TCounter> counters;

    const int nPools = 500;
    const int nPoolTrees = 5;
    const int nSlots = 10;
    const int nPoolGauges = 50;
    const int nPoolCounters = 50;

    for (int pool = 0; pool < nPools; ++pool) {
        for (int slotIndex = 0; slotIndex < nSlots; ++slotIndex) {
            auto poolRegistry = r
                .WithGlobal()
                .WithTag("pool_tree", ToString(pool % nPoolTrees))
                .WithTag("pool", ToString(pool), -1)
                .WithTag("slot_index", ToString(slotIndex), -1);

            for (int metric = 0; metric < nPoolGauges; ++metric) {
                gauges.push_back(poolRegistry.Gauge(Format("/metric_gauge_%d", metric)));
            }
            for (int metric = 0; metric < nPoolCounters; ++metric) {
                counters.push_back(poolRegistry.Counter(Format("/metric_counter_%d", metric)));
            }
        }
    }

    return std::pair(gauges, counters);
}

////////////////////////////////////////////////////////////////////////////////

void CheckErrors(const TSolomonRegistryPtr& impl)
{
    auto list = impl->ListSensors();

    for (const auto& s : list) {
        s.Error.ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TWorkload>
void BM_SolomonCollect(benchmark::State& state, TWorkload workload)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler{impl, "/benchmark"};

    auto counters = workload(profiler);
    impl->ProcessRegistrations();
    CheckErrors(impl);

    for (auto _ : state) {
        impl->Collect();
    }
}

BENCHMARK_CAPTURE(BM_SolomonCollect, Rpc, RpcWorkload)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SolomonCollect, Scheduler, SchedulerWorkload)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SolomonCollect, MethodLatency, MethodLatencyWorkload)
    ->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

template <class TWorkload>
void BM_SolomonRead(benchmark::State& state, TWorkload workload)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler{impl, "/benchmark"};

    auto counters = workload(profiler);
    impl->ProcessRegistrations();
    CheckErrors(impl);

    impl->Collect();

    auto now = TInstant::Now();
    TReadOptions options;
    options.Times = {{{0}, now}};

    for (auto _ : state) {
        TBufferOutput buffer;
        auto spack = NMonitoring::EncoderSpackV1(
            &buffer,
            NMonitoring::ETimePrecision::SECONDS,
            NMonitoring::ECompression::LZ4);

        spack->OnStreamBegin();

        impl->ReadSensors(options, spack.Get());

        spack->OnStreamEnd();
        spack->Close();
    }
}

BENCHMARK_CAPTURE(BM_SolomonRead, Rpc, RpcWorkload)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SolomonRead, Scheduler, SchedulerWorkload)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SolomonRead, MethodLatency, MethodLatencyWorkload)
    ->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

template <bool isHot>
void BM_PerCpuCounter(benchmark::State& state)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler{impl, "/benchmark"};

    if (isHot) {
        profiler = profiler.WithHot();
    }

    static auto counter = profiler.Counter("/test");

    for (auto _ : state) {
        counter.Increment();
    }
}

BENCHMARK_TEMPLATE(BM_PerCpuCounter, false)
    ->Threads(1)
    ->Threads(16);

BENCHMARK_TEMPLATE(BM_PerCpuCounter, true)
    ->Threads(1)
    ->Threads(16);

////////////////////////////////////////////////////////////////////////////////

template <bool isHot>
void BM_PerCpuGauge(benchmark::State& state)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler{impl, "/benchmark"};

    if (isHot) {
        profiler = profiler.WithHot();
    }

    static auto gauge = profiler.Gauge("/test");

    for (auto _ : state) {
        gauge.Update(1.0);
    }
}

BENCHMARK_TEMPLATE(BM_PerCpuGauge, false)
    ->Threads(1)
    ->Threads(16);

BENCHMARK_TEMPLATE(BM_PerCpuGauge, true)
    ->Threads(1)
    ->Threads(16);

////////////////////////////////////////////////////////////////////////////////

template <bool isHot>
void BM_PerCpuSummary(benchmark::State& state)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler{impl, "/benchmark"};

    if (isHot) {
        profiler = profiler.WithHot();
    }

    static auto summary = profiler.Summary("/test");

    for (auto _ : state) {
        summary.Record(1.0);
    }
}

BENCHMARK_TEMPLATE(BM_PerCpuSummary, false)
    ->Threads(1)
    ->Threads(16);

BENCHMARK_TEMPLATE(BM_PerCpuSummary, true)
    ->Threads(1)
    ->Threads(16);

////////////////////////////////////////////////////////////////////////////////

void BM_SpackWriter(benchmark::State& state, NMonitoring::ECompression compression)
{
    constexpr int nLabels = 3;

    auto n = state.range(0);

    for (auto _ : state) {
        TStringStream buffer;
        auto encoder = NMonitoring::EncoderSpackV1(
            &buffer,
            NMonitoring::ETimePrecision::SECONDS,
            compression,
            NMonitoring::EMetricsMergingMode::MERGE_METRICS);

        auto now = TInstant::Now();
        encoder->OnStreamBegin();

        for (int i = 0; i < n; ++i) {
            encoder->OnMetricBegin(NMonitoring::EMetricType::GAUGE);

            encoder->OnLabelsBegin();
            for (int j = 0; j < nLabels; ++j) {
                encoder->OnLabel(Format("key_%d", j), Format("value_%d_%d", j, i));
            }
            encoder->OnLabelsEnd();

            encoder->OnDouble(now, 1.0);

            encoder->OnMetricEnd();
        }

        encoder->OnStreamEnd();
        encoder->Close();
    }
}

BENCHMARK_CAPTURE(BM_SpackWriter, IDENTITY, NMonitoring::ECompression::IDENTITY)
    ->Range(10'000, 100'000)
    ->Unit(::benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SpackWriter, LZ4, NMonitoring::ECompression::LZ4)
    ->Range(10'000, 100'000)
    ->Unit(::benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_SpackWriter, ZSTD, NMonitoring::ECompression::ZSTD)
    ->Range(10'000, 100'000)
    ->Unit(::benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
