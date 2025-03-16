#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <library/cpp/yt/memory/safe_memory_reader.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/cursors/dummy/dummy_cursor.h>

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <util/generic/buffer.h>

#include <util/generic/size_literals.h>

#include <util/system/compiler.h>

#include <contrib/libs/libunwind/include/libunwind.h>

namespace NYT::NBacktrace {
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

        DoNotOptimizeAway(touchMem);
    }
}

TEST(TFramePointerCursor, FramePointerCursor)
{
    std::vector<const void*> backtrace;
    RunInDeepStack<64>([&] {
        unw_context_t unwContext;
        ASSERT_TRUE(unw_getcontext(&unwContext) == 0);

        unw_cursor_t unwCursor;
        ASSERT_TRUE(unw_init_local(&unwCursor, &unwContext) == 0);

        TSafeMemoryReader reader;
        auto fpCursorContext = NBacktrace::FramePointerCursorContextFromLibunwindCursor(unwCursor);
        NBacktrace::TFramePointerCursor fpCursor(&reader, fpCursorContext);

        while (!fpCursor.IsFinished()) {
            backtrace.push_back(fpCursor.GetCurrentIP());
            fpCursor.MoveNext();
        }
    });

    ASSERT_THAT(backtrace, testing::SizeIs(testing::Ge(64u)));
}

////////////////////////////////////////////////////////////////////////////////

template <class TFn>
Y_NO_INLINE void RunInFiber(TFn fn)
{
    struct TFiber
        : public ITrampoLine
    {
        TFiber(TFn fn) noexcept
            : Fn_(fn)
            , Stack_(4_KB)
            , Context_({this, TArrayRef(Stack_.Data(), Stack_.Capacity())})
        { }

        void Run()
        {
            CallerContext_.SwitchTo(&Context_);
        }

        void DoRun() override
        {
            (*Fn_)();
            Context_.SwitchTo(&CallerContext_);
        }

        TFn Fn_;
        TBuffer Stack_;
        TExceptionSafeContext Context_;
        TExceptionSafeContext CallerContext_;
    };

    TFiber fiber(fn);
    fiber.Run();
}

template <class TCursor>
class TCursorTest
    : public ::testing::Test
{ };

using TCursorTypes = ::testing::Types<
    TDummyCursor,
    // FIXME(khlebnikov): It needs faster memory access and context constructor.
    // TFramePointerCursor,
    TLibunwindCursor
>;

TYPED_TEST_SUITE(TCursorTest, TCursorTypes);

TYPED_TEST(TCursorTest, Simple)
{
    using TCursor = TypeParam;

    TCursor cursor;
    std::array<const void*, 16> buffer;

    auto frames = GetBacktrace(&cursor, TMutableRange(buffer), 0);
    Y_UNUSED(frames);
}

TYPED_TEST(TCursorTest, InFiber)
{
    using TCursor = TypeParam;

    RunInFiber([] {
        TCursor cursor;
        std::array<const void*, 16> buffer;

        auto frames = GetBacktrace(&cursor, TMutableRange(buffer), 0);
        Y_UNUSED(frames);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktrace
