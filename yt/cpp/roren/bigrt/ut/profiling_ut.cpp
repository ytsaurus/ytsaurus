#include <yt/cpp/roren/bigrt/profiling.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;
using namespace NSFStats;
using namespace NYT::NProfiling;

static TString FormatStats(TStats& s) {
    TStringStream b;
    s.Out(TSolomonOut(b));
    return b.Str();
}

TEST(Profiling, Simple) {
    TStats s;
    {
        TStats::TContext ctx(s);
        TSolomonContext sctx(ctx);
        auto profiler = NRoren::NPrivate::CreateSolomonContextProfiler(sctx);
        auto counter = profiler.Counter("metric");
        counter.Increment(5);
    }
    TString answer = "{\"sensors\":[{\"labels\":{\"sensor\":\"metric\"},\"mode\":\"deriv\",\"value\":5}]}";
    ASSERT_EQ(FormatStats(s), answer);
}

TEST(Profiling, Unwrap) {
    TStats s;
    {
        TStats::TContext ctx(s);
        TSolomonContext sctx(ctx);
        auto profiler = NRoren::NPrivate::CreateSolomonContextProfiler(sctx);
        auto counter = profiler.Counter("metric");
        counter.Increment(5);

        auto sctx2 = NRoren::NPrivate::UnwrapSolomonContextProfiler(profiler);
        sctx2.Inc("metric", 7);
    }
    TString answer = "{\"sensors\":[{\"labels\":{\"sensor\":\"metric\"},\"mode\":\"deriv\",\"value\":12}]}";
    ASSERT_EQ(FormatStats(s), answer);
}
