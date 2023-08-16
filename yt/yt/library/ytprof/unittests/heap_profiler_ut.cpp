#include <gtest/gtest.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/tracing/allocation_tags.h>
#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/testing/common/env.h>

#include <util/string/cast.h>
#include <util/stream/file.h>
#include <util/generic/hash_set.h>
#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

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
    for (int i = 0; i < 10240; i++) {
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

    traceContext->SetAllocationTags({{"user", "first"}, {"sometag", "my"}});

    auto h0 = BlowHeap<0>();

    auto tag = TMemoryTag(1);
    traceContext->SetAllocationTags({{"user", "second"}, {"sometag", "notmy"}, {MemoryTagLiteral, ToString(tag)}});
    auto currentTag = traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral);
    ASSERT_EQ(currentTag, tag);

    auto h1 = BlowHeap<1>();

    traceContext->ClearAllocationTagsPtr();

    auto h2 = BlowHeap<2>();
    h2.clear();

    auto usage = GetEstimatedMemoryUsage();
    ASSERT_GE(usage[tag], 5_MB);

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

TEST(HeapProfiler, AllocationTagsWithMemoryTag)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(256_KB);

    auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    enum EMemoryTags {
        MT_0 = NullMemoryTag,
        MT_1,
        MT_2,
        MT_3,
        MT_4,
        MT_5,
        MT_6,
        MT_7
    };

    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), std::nullopt);
    traceContext->SetAllocationTags({{"user", "first user"}, {NTracing::MemoryTagLiteral, ToString(0)}});
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("user"), "first user");
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), MT_0);

    std::vector<std::vector<TString>> heap;
    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTags({{"user", "second user"}, {NTracing::MemoryTagLiteral, ToString(1)}});
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), MT_1);

    heap.push_back(BlowHeap<1>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_0);

    auto usage1 = GetEstimatedMemoryUsage()[MT_1];
    ASSERT_GE(usage1, 9_MB);
    ASSERT_LE(usage1, 16_MB);

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_2);
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), MT_2);

    {
        volatile auto h = BlowHeap<2>();
    }

    traceContext->ClearAllocationTagsPtr();
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), std::nullopt);

    heap.push_back(BlowHeap<0>());

    {
        auto usage = GetEstimatedMemoryUsage();
        ASSERT_EQ(usage[MT_1], usage1);
        ASSERT_LE(usage[MT_2], 1_MB);
    }

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_7);

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_3);
    heap.push_back(BlowHeap<3>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_4);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_7);

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_5);
    heap.push_back(BlowHeap<5>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_4);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_6);
    heap.push_back(BlowHeap<6>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_0);

    auto usage = GetEstimatedMemoryUsage();
    auto maxDifference = 5_MB;
    ASSERT_NEAR(usage[MT_1], usage[MT_3], maxDifference);
    ASSERT_NEAR(usage[MT_3], usage[MT_5], maxDifference);
    ASSERT_NEAR(usage[MT_1], usage[MT_5], maxDifference);
    ASSERT_GE(usage[MT_4], 19_MB);
    ASSERT_LE(usage[MT_4], 27_MB);
    ASSERT_NEAR(usage[MT_4], usage[MT_1] +  usage[MT_5], 4_MB);
}

template <size_t Index>
Y_NO_INLINE auto BlowHeap(int64_t megabytes)
{
    std::vector<TString> data;
    megabytes <<= 10;
    for (int64_t i = 0; i < megabytes; i++) {
        data.push_back(TString( 1024, 'x'));
    }
    return data;
}

TEST(HeapProfiler, HugeAllocationsTagsWithMemoryTag)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(256_KB);

    auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TCurrentTraceContextGuard guard(traceContext);

    enum EMemoryTags {
        MT_0 = NullMemoryTag,
        MT_1,
        MT_2
    };

    std::vector<std::vector<TString>> heap;

    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_1);
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryTagLiteral), MT_1);

    heap.push_back(BlowHeap<1>(100));

    {
        traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_0);
        auto usage = GetEstimatedMemoryUsage()[MT_1];
        ASSERT_GE(usage, 100_MB);
        ASSERT_LE(usage, 130_MB);
    }

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_2);
    heap.push_back(BlowHeap<1>(1000));

    traceContext->SetAllocationTag<TMemoryTag>(MemoryTagLiteral, MT_0);
    auto usage = GetEstimatedMemoryUsage()[MT_2];
    ASSERT_GE(usage, 1000_MB);
    ASSERT_LE(usage, 1200_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
