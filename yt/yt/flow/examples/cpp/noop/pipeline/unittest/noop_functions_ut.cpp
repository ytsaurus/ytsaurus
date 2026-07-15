#include <yt/yt/flow/examples/cpp/noop/pipeline/lib/noop_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

TEST(TNoopFunctionExampleTest, EmitsNothing)
{
    TTestStateEnvironment stateEnv;
    TProcessFunctionTestHarness harness(stateEnv, New<TNoopFunction>());

    auto emptySchema = New<TTableSchema>();
    harness.RunEpoch({MakeTestMessage("random", MakeKey<ui64>(0), emptySchema)});

    EXPECT_TRUE(harness.GetMessages().empty());
    EXPECT_TRUE(harness.GetTimers().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
