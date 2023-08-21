#include <gtest/gtest.h>

#include <util/string/cast.h>
#include <util/stream/file.h>
#include <util/generic/hash_set.h>
#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <library/cpp/testing/common/env.h>

#include <yt/yt/core/tracing/allocation_tags.h>
#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>

#include <tcmalloc/common.h>

#include <absl/debugging/stacktrace.h>

namespace NYT::NYTProf {
namespace {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

template <size_t Index>
Y_NO_INLINE auto BlowHeap()
{
    std::vector<TString> data;
    for (int i = 0; i < 10000; i++) {
        data.push_back(TString(1024, 'x'));
    }
    return data;
}

TEST(HeapProfiler, ReadProfile)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(256_KB);

    auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    traceContext->SetAllocationTags(New<TAllocationTags>(TAllocationTags::TTags({{"user", "first"}, {"sometag", "my"}})));

    auto h0 = BlowHeap<0>();

    SetMemoryTag(1);
    traceContext->SetAllocationTags(New<TAllocationTags>(TAllocationTags::TTags({{"user", "second"}, {"sometag", "notmy"}})));

    auto h1 = BlowHeap<1>();

    SetMemoryTag(0);
    traceContext->SetAllocationTags(nullptr);

    auto h2 = BlowHeap<2>();
    h2.clear();

    auto usage = GetEstimatedMemoryUsage();
    ASSERT_GE(usage[1], 5_MB);

    auto dumpProfile = [] (auto name, auto type) {
        auto profile = ReadHeapProfile(type);

        TFileOutput output(GetOutputPath() / name);
        WriteProfile(&output, profile);
        output.Finish();
    };

    dumpProfile("heap.pb.gz", tcmalloc::ProfileType::kHeap);
    dumpProfile("peak.pb.gz", tcmalloc::ProfileType::kPeakHeap);
    dumpProfile("fragmentation.pb.gz", tcmalloc::ProfileType::kFragmentation);
    dumpProfile("allocations.pb.gz", tcmalloc::ProfileType::kAllocations);

    auto profile = std::move(token).Stop();

    TFileOutput output(GetOutputPath() / "allocations.pb.gz");
    WriteProfile(&output, ConvertAllocationProfile(profile));
    output.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
