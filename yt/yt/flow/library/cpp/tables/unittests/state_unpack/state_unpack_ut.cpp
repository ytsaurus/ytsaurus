#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/resource/resource.h>

namespace NYT::NFlow::NTables {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TInternalStateRowTest, UnpackGoldenRows)
{
    auto cases = ConvertTo<IListNodePtr>(TYsonString(NResource::Find("flow_internal_state_rows")));
    for (const auto& node : cases->GetChildren()) {
        auto testCase = node->AsMap();
        SCOPED_TRACE(ConvertTo<std::string>(testCase->GetChildOrThrow("name")));
        auto row = testCase->GetChildOrThrow("row")->AsMap();
        if (ConvertTo<bool>(testCase->GetChildOrThrow("expect_error"))) {
            EXPECT_THROW(UnpackInternalState(row), std::exception);
            continue;
        }
        auto unpacked = UnpackInternalState(row);
        EXPECT_TRUE(AreNodesEqual(unpacked, testCase->GetChildOrThrow("expected_state")));
    }
}

TEST(TInternalStateRowTest, ReadyStateShortCircuits)
{
    for (auto value : {"%false", "0", "\"\"", "[]", "{}"}) {
        auto row = ConvertTo<IMapNodePtr>(TYsonStringBuf("{compressed=42;format=%false}"));
        auto state = ConvertToNode(TYsonStringBuf(value));
        row->AddChild("state", state);
        EXPECT_EQ(UnpackInternalState(row), state);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NTables
