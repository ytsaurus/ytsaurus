#include <library/cpp/framing/packer.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <library/cpp/framing/benchmark/test.pb.h>

using namespace NFraming;

static void BM_PackStringToStream(benchmark::State& state) {
    auto format = static_cast<EFormat>(state.range(0));
    for (auto _ : state) {
        TStringStream out;
        TPacker packer(format, out);
        packer.Add(TString{4096, 'a'});
        packer.Flush();
    }
}

static void BM_PackProtoToStream(benchmark::State& state) {
    NFramingBench::TestData msg;
    for (auto j : xrange(255u)) {
        msg.mutable_a()->add_ints(static_cast<ui32>(j));
        msg.mutable_a()->add_floats(static_cast<float>(j) * 0.01f);
    }

    auto format = static_cast<EFormat>(state.range(0));
    for (auto _ : state) {
        TStringStream out;
        TPacker packer(format, out);
        packer.Add(msg);
        packer.Flush();
    }
}

static void BM_PackStringToString(benchmark::State& state) {
    auto format = static_cast<EFormat>(state.range(0));
    for (auto _ : state) {
        benchmark::DoNotOptimize(PackToString(format, TString{4096, 'a'}));
    }
}

static void BM_PackProtoToString(benchmark::State& state) {
    NFramingBench::TestData msg;
    for (auto j : xrange(255u)) {
        msg.mutable_a()->add_ints(static_cast<ui32>(j));
        msg.mutable_a()->add_floats(static_cast<float>(j) * 0.01f);
    }
    Y_UNUSED(msg.ByteSize());

    auto format = static_cast<EFormat>(state.range(0));
    auto useCachedSize = static_cast<bool>(state.range(1));
    for (auto _ : state) {
        benchmark::DoNotOptimize(PackToString(format, msg, useCachedSize));
    }
}

BENCHMARK(BM_PackStringToStream)->Arg(static_cast<int>(EFormat::Protoseq))->Arg(static_cast<int>(EFormat::Lenval))->Arg(static_cast<int>(EFormat::LightProtoseq));

BENCHMARK(BM_PackProtoToStream)->Arg(static_cast<int>(EFormat::Protoseq))->Arg(static_cast<int>(EFormat::Lenval))->Arg(static_cast<int>(EFormat::LightProtoseq));

BENCHMARK(BM_PackStringToString)->Arg(static_cast<int>(EFormat::Protoseq))->Arg(static_cast<int>(EFormat::Lenval))->Arg(static_cast<int>(EFormat::LightProtoseq));

BENCHMARK(BM_PackProtoToString)
    ->Args({static_cast<int>(EFormat::Protoseq), 0})
    ->Args({static_cast<int>(EFormat::Protoseq), 1})
    ->Args({static_cast<int>(EFormat::Lenval), 0})
    ->Args({static_cast<int>(EFormat::Lenval), 1})
    ->Args({static_cast<int>(EFormat::LightProtoseq), 0})
    ->Args({static_cast<int>(EFormat::LightProtoseq), 1});
