#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/companion_client.h>
#include <yt/yt/flow/library/cpp/companion/companion_model.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionClientTest, TestParseTCompanionStatus)
{
    auto result = NYT::NYTree::ConvertTo<TCompanionInfoPtr>(TYsonString(TStringBuf(R""""(
        {
            computations={
                "computation_id_1"={
                    computation_id="computation_id_1";
                    computation_type="Source";
                };
                "computation_id_2"={
                    computation_id="computation_id_2";
                    computation_type="Source";
                };
                "computation_id_3"={
                    computation_id="computation_id_3";
                    computation_type="Transform";
                };
                "computation_id_4"={
                    computation_id="computation_id_4";
                    computation_type="Source";
                };
            };
        }
    )"""")));
    auto computation1 = result->Computations[TComputationId("computation_id_1")];
    EXPECT_EQ(ECompanionComputationType::Source, computation1->CompanionComputationType);
    auto computation2 = result->Computations[TComputationId("computation_id_2")];
    EXPECT_EQ(ECompanionComputationType::Source, computation2->CompanionComputationType);
    auto computation3 = result->Computations[TComputationId("computation_id_3")];
    EXPECT_EQ(ECompanionComputationType::Transform, computation3->CompanionComputationType);
    auto computation4 = result->Computations[TComputationId("computation_id_4")];
    EXPECT_EQ(ECompanionComputationType::Source, computation4->CompanionComputationType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
