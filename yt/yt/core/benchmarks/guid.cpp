#include <yt/yt/core/misc/guid.h>

#include <library/cpp/yt/string/format.h>

#include <benchmark/benchmark.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto Guid = TGuid::FromString("12345678-abcdabcd-12345678-abcdabcd");

void Guid_Sprintf(benchmark::State& state)
{
    TStringBuilder builder;
    while (state.KeepRunning()) {
        char* buf = builder.Preallocate(3 + 4 * 8);
        int count = sprintf(buf, "%x-%x-%x-%x",
            Guid.Parts32[3],
            Guid.Parts32[2],
            Guid.Parts32[1],
            Guid.Parts32[0]);
        builder.Advance(count);
    }
}

BENCHMARK(Guid_Sprintf);

void Guid_Format(benchmark::State& state)
{
    TStringBuilder builder;
    builder.Preallocate(256 * 1024 * 1024);
    while (state.KeepRunning()) {
        FormatValue(&builder, Guid, TStringBuf("v"));
    }
}

BENCHMARK(Guid_Format);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
