#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/yson/benchmark/proto/sample.pb.h>

#include <benchmark/benchmark.h>

using namespace NYT::NYson;

namespace {

////////////////////////////////////////////////////////////////////////////////

void ReflectProtobufMessageTypeBenchmark(benchmark::State& state)
{
    for (auto _ : state) {
        ReflectProtobufMessageType(NYT::NYson::NProto::TMessage::GetDescriptor());
    }
}

BENCHMARK(ReflectProtobufMessageTypeBenchmark)
    ->ThreadRange(1, 8)
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

void ReflectProtobufEnumTypeBenchmark(benchmark::State& state)
{
    for (auto _ : state) {
        ReflectProtobufEnumType(NYT::NYson::NProto::EColor_descriptor());
    }
}

BENCHMARK(ReflectProtobufEnumTypeBenchmark)
    ->ThreadRange(1, 8)
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

} // namespace
