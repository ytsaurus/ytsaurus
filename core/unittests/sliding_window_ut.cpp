#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/sliding_window.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size = 0>
class TSlidingWindowTest
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        auto callback = BIND([&] (size_t&& value) {
            ReceivedElements_.push_back(value);
        });

        Window_ = std::make_unique<TSlidingWindow<size_t, Size>>(callback);
    }

    void PushElement(size_t value)
    {
        Window_->AddPacket(value, size_t(value));
    }

    bool CheckElements(size_t minValue, size_t expectedSize)
    {
        if (ReceivedElements_.size() != expectedSize ||
            Window_->GetNextSequenceNumber() != expectedSize)
        {
            return false;
        }

        for (size_t value = minValue; value < expectedSize; ++value) {
            if (ReceivedElements_[value] != value) {
                return false;
            }
        }
        return true;
    }

protected:
    std::unique_ptr<TSlidingWindow<size_t, Size>> Window_;
    std::vector<size_t> ReceivedElements_;
};

using TSingleSlidingWindowTest = TSlidingWindowTest<1>;

TEST_F(TSingleSlidingWindowTest, TSingleTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->Empty());

    EXPECT_ANY_THROW(PushElement(1));
    EXPECT_ANY_THROW(PushElement(2));
    PushElement(0);
    PushElement(1);
    EXPECT_ANY_THROW(PushElement(1));
    PushElement(2);
    EXPECT_TRUE(CheckElements(0, 3));
    EXPECT_ANY_THROW(PushElement(4));
    EXPECT_TRUE(Window_->Empty());
}

const size_t windowSize = 1024;

using TFiniteSlidingWindowTest = TSlidingWindowTest<windowSize>;

TEST_F(TFiniteSlidingWindowTest, TFiniteTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->Empty());

    EXPECT_ANY_THROW(PushElement(windowSize));
    PushElement(0);
    for (auto element = windowSize; element > 1; --element) {
        PushElement(element);
    }
    EXPECT_TRUE(CheckElements(0, 1));
    EXPECT_FALSE(Window_->Empty());
    EXPECT_ANY_THROW(PushElement(2));
    EXPECT_ANY_THROW(PushElement(windowSize));

    PushElement(1);
    EXPECT_TRUE(CheckElements(1, windowSize + 1));
    EXPECT_TRUE(Window_->Empty());

    EXPECT_ANY_THROW(PushElement(2 * windowSize + 1));
    PushElement(2 * windowSize);
    EXPECT_TRUE(CheckElements(windowSize + 1, windowSize + 1));
    EXPECT_FALSE(Window_->Empty());
}

using TInfiniteSlidingWindowTest = TSlidingWindowTest<>;

TEST_F(TInfiniteSlidingWindowTest, TInfiniteTest)
{
    EXPECT_TRUE(CheckElements(0, 0));
    EXPECT_TRUE(Window_->Empty());

    PushElement(windowSize);
    PushElement(0);
    for (auto element = windowSize - 1; element > 1; --element) {
        PushElement(element);
    }
    EXPECT_TRUE(CheckElements(0, 1));
    EXPECT_FALSE(Window_->Empty());
    EXPECT_ANY_THROW(PushElement(2));
    EXPECT_ANY_THROW(PushElement(windowSize));

    PushElement(1);
    EXPECT_TRUE(CheckElements(1, windowSize + 1));
    EXPECT_TRUE(Window_->Empty());

    PushElement(std::numeric_limits<size_t>::max());
    EXPECT_ANY_THROW(PushElement(std::numeric_limits<size_t>::max()));
    EXPECT_TRUE(CheckElements(windowSize + 1, windowSize + 1));
    EXPECT_FALSE(Window_->Empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

