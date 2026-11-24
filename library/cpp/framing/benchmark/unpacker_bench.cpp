#include <library/cpp/framing/packer.h>
#include <library/cpp/framing/unpacker.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <library/cpp/framing/benchmark/test.pb.h>

using namespace NFraming;

static void BM_UnpackString(benchmark::State& state) {
    auto format = static_cast<EFormat>(state.range(0));
    const TString packed = PackToString(format, TString{4096, 'a'});
    for (auto _ : state) {
        TUnpacker unpacker(format, packed);
        TStringBuf frame, skip;
        if (!unpacker.NextFrame(frame, skip) || !skip.empty()) {
            state.SkipWithError("Unpacker broken!");
        }
        if (unpacker.NextFrame(frame, skip)) {
            state.SkipWithError("Unpacker broken!");
        }
    }
}

static void BM_UnpackProto(benchmark::State& state) {
    NFramingBench::TestData msg;
    for (auto j : xrange(255u)) {
        msg.mutable_a()->add_ints(static_cast<ui32>(j));
        msg.mutable_a()->add_floats(static_cast<float>(j) * 0.01f);
    }

    auto format = static_cast<EFormat>(state.range(0));
    const TString packed = PackToString(format, msg);
    for (auto _ : state) {
        TUnpacker unpacker(format, packed);
        TStringBuf skip;
        if (!unpacker.NextFrame(msg, skip) || !skip.empty()) {
            state.SkipWithError("Unpacker broken!");
        }
        if (unpacker.NextFrame(msg, skip)) {
            state.SkipWithError("Unpacker broken!");
        }
    }
}

BENCHMARK(BM_UnpackString)->Arg(static_cast<int>(EFormat::Protoseq))->Arg(static_cast<int>(EFormat::Lenval))->Arg(static_cast<int>(EFormat::LightProtoseq));

BENCHMARK(BM_UnpackProto)->Arg(static_cast<int>(EFormat::Protoseq))->Arg(static_cast<int>(EFormat::Lenval))->Arg(static_cast<int>(EFormat::LightProtoseq));
