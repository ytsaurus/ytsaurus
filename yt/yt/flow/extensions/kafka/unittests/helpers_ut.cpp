#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/helpers.h>

#include <stdexcept>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCreateUntilTerminatedTest, RetriesUntilTheFactorySucceeds)
{
    std::atomic<bool> terminated = false;
    int attempts = 0;
    std::vector<std::string> errors;

    auto value = CreateUntilTerminated<int>(
        [&] {
            if (++attempts < 3) {
                throw std::runtime_error("attempt " + std::to_string(attempts));
            }
            return std::make_unique<int>(42);
        },
        [&] (const std::exception& ex) {
            errors.emplace_back(ex.what());
        },
        terminated,
        TDuration::Zero());

    ASSERT_TRUE(value);
    EXPECT_EQ(*value, 42);
    EXPECT_EQ(attempts, 3);
    EXPECT_EQ(errors, (std::vector<std::string>{"attempt 1", "attempt 2"}));
}

TEST(TCreateUntilTerminatedTest, GivesUpOnceTerminated)
{
    std::atomic<bool> terminated = false;
    int attempts = 0;

    auto value = CreateUntilTerminated<int>(
        [&] () -> std::unique_ptr<int> {
            throw std::runtime_error("unavailable");
        },
        [&] (const std::exception& /*ex*/) {
            // Termination arrives before the backoff: it must not be waited out.
            ++attempts;
            terminated.store(true);
        },
        terminated,
        TDuration::Hours(1));

    EXPECT_FALSE(value);
    EXPECT_EQ(attempts, 1);
}

TEST(TCreateUntilTerminatedTest, DoesNotStartOnceTerminated)
{
    std::atomic<bool> terminated = true;
    bool called = false;

    auto value = CreateUntilTerminated<int>(
        [&] {
            called = true;
            return std::make_unique<int>(0);
        },
        [&] (const std::exception& /*ex*/) {
        },
        terminated,
        TDuration::Zero());

    EXPECT_FALSE(value);
    EXPECT_FALSE(called);
}

TEST(TSleepUnlessTerminatedTest, ReturnsAtOnceWhenTerminated)
{
    std::atomic<bool> terminated = true;
    auto start = TInstant::Now();

    SleepUnlessTerminated(terminated, TDuration::Hours(1));

    EXPECT_LT(TInstant::Now() - start, TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
