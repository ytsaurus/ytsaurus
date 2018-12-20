#include <yt/core/test_framework/framework.h>

#include "operation_controller.h"

#include <yt/core/yson/null_consumer.h>

#include <contrib/libs/gmock/gmock/gmock.h>
#include <contrib/libs/gmock/gmock/gmock-matchers.h>
#include <contrib/libs/gmock/gmock/gmock-actions.h>


namespace NYT::NSchedulerSimulator {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

class TControlThreadTest
    : public testing::Test
{
protected:

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControlThreadTest, TestAssertTrue)
{
    EXPECT_TRUE(true);
}

} // namespace
} // namespace NYT::NSchedulerSimulator
