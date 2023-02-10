#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <benchmark/benchmark.h>

#include <yt/yt/core/misc/safe_memory_reader.h>

#include <yt/yt/library/ytprof/backtrace.h>

#include <util/system/compiler.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <int Depth, class TFn>
Y_NO_INLINE void RunInDeepStack(TFn cb)
{
    if constexpr (Depth == 0) {
        cb();
    } else {
        std::vector<int> touchMem;
        touchMem.push_back(0);

        RunInDeepStack<Depth-1>(cb);

        benchmark::DoNotOptimize(touchMem);
    }
}

TEST(TFramePointerCursor, FramePointerCursor)
{
    if (!IsProfileBuild()) {
        return;
    }

    std::vector<void*> backtrace;
    RunInDeepStack<64>([&] {
        unw_context_t context;
        ASSERT_TRUE(unw_getcontext(&context) == 0);

        unw_cursor_t cursor;
        ASSERT_TRUE(unw_init_local(&cursor, &context) == 0);

        unw_word_t ip = 0;
        ASSERT_TRUE(unw_get_reg(&cursor, UNW_REG_IP, &ip) == 0);

        unw_word_t rsp = 0;
        ASSERT_TRUE(unw_get_reg(&cursor, UNW_X86_64_RSP, &rsp) == 0);

        unw_word_t rbp = 0;
        ASSERT_TRUE(unw_get_reg(&cursor, UNW_X86_64_RBP, &rbp) == 0);

        TSafeMemoryReader mem;
        TFramePointerCursor fpCursor(
            &mem,
            reinterpret_cast<void*>(ip),
            reinterpret_cast<void*>(rsp),
            reinterpret_cast<void*>(rbp));

        while (!fpCursor.IsEnd()) {
            backtrace.push_back(fpCursor.GetIP());
            fpCursor.Next();
        }
    });

    ASSERT_THAT(backtrace, testing::SizeIs(testing::Ge(64u)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
