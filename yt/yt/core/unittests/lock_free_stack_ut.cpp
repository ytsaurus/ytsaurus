#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/format.h>
#include <yt/core/misc/new.h>
#include <yt/core/misc/public.h>
#include <yt/core/misc/lock_free_stack.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLockFreeStackTests, TestDequeueAll)
{
    TLockFreeStack<int> stack;

    stack.Enqueue(17);
    stack.Enqueue(19);
    stack.Enqueue(23);

    std::vector<int> r;

    stack.DequeueAll([&] (int item) {
        r.push_back(item);
    });

    EXPECT_EQ(size_t(3), r.size());
    EXPECT_EQ(23, r.at(0));
    EXPECT_EQ(19, r.at(1));
    EXPECT_EQ(17, r.at(2));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
