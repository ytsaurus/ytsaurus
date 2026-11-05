#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/transform_ordered_source_companion_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TTransformOrderedSourceCompanionComputationTest, RegistersParameters)
{
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(R"(
        {
            computation_class_name = "NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation";
            parameters = {
                internal_states = [first; second];
            };
        }
    )"));

    auto parameters = DynamicPointerCast<TTransformOrderedSourceCompanionParameters>(
        TRegistry::Get()->ParseComputationParameters(spec));

    ASSERT_TRUE(parameters);
    ASSERT_TRUE(parameters->InternalStates);
    EXPECT_EQ(2u, parameters->InternalStates->size());
    EXPECT_TRUE(parameters->InternalStates->contains("first"));
    EXPECT_TRUE(parameters->InternalStates->contains("second"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
