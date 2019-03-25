#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/lock_free.h>

#include <util/system/thread.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSpscQueueTest
    : public ::testing::Test
{
protected:
    TSingleProducerSingleConsumerQueue<int> A;
    TSingleProducerSingleConsumerQueue<int> B;

    std::atomic<bool> Stopped = {false};

    TSpscQueueTest()
    { }

    void AtoB()
    {
        int item;
        while (A.Pop(&item) && !Stopped.load()) {
            B.Push(std::move(item));
        }
    }

    void BtoA()
    {
        int item;
        while (B.Pop(&item) && !Stopped.load()) {
            A.Push(std::move(item));
        }
    }

};

TEST_F(TSpscQueueTest, TwoThreads)
{
    constexpr int N = 1000;

    using TThis = typename std::remove_reference<decltype(*this)>::type;
    TThread thread1([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        this_->AtoB();
        return nullptr;
    }, this);
    TThread thread2([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        this_->BtoA();
        return nullptr;
    }, this);

    for (size_t i = 1; i <= N; ++i) {
        if (i & 1) {
            A.Push(i);
        } else {
            B.Push(i);
        }
    }

    thread1.Start();
    thread2.Start();

    Sleep(TDuration::Seconds(2));

    Stopped.store(true);

    thread1.Join();
    thread2.Join();

    std::vector<int> elements;
    int item;
    while (A.Pop(&item)) {
        elements.push_back(item);
    }
    while (B.Pop(&item)) {
        elements.push_back(item);
    }

    std::sort(elements.begin(), elements.end());

    EXPECT_EQ(elements.size(), N);
    for (size_t i = 1; i <= N; ++i) {
        EXPECT_EQ(elements[i - 1], i);
    }

}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
