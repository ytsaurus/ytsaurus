#include <yt/yql/plugin/lib/progress_merger.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NYqlPlugin {
namespace {

using namespace NYTree;
using EState = NYql::TOperationProgress::EState;

TEST(TProgressMergerTest, FinalizesOnlyUnfinishedNodes)
{
    TProgressMerger merger;
    merger.MergeWith(NYql::TOperationProgress("yt", 1, EState::Finished));
    merger.MergeWith(NYql::TOperationProgress("yt", 2, EState::Started));
    NYql::TOperationProgress progress("yt", 2, EState::InProgress);
    progress.RemoteId = "cluster/operation";
    merger.MergeWith(progress);
    merger.MergeWith(NYql::TOperationProgress("yt", 2, EState::Failed));
    merger.MergeWith(NYql::TOperationProgress("yt", 3, EState::Started));
    merger.MergeWith(NYql::TOperationProgress("yt", 4, EState::InProgress));

    merger.ToYsonString();
    EXPECT_FALSE(merger.HasChangesSinceLastFlush());
    merger.AbortAllUnfinishedNodes();
    EXPECT_TRUE(merger.HasChangesSinceLastFlush());

    auto nodes = ConvertToNode(NYson::TYsonString(merger.ToYsonString()))->AsMap();
    EXPECT_EQ(nodes->GetChildOrThrow("1")->AsMap()->GetChildValueOrThrow<TString>("state"), "Finished");
    auto failed = nodes->GetChildOrThrow("2")->AsMap();
    EXPECT_EQ(failed->GetChildValueOrThrow<TString>("state"), "Failed");
    EXPECT_EQ(failed->GetChildValueOrThrow<TString>("remoteId"), "cluster/operation");
    for (const auto& id : {"3", "4"}) {
        EXPECT_EQ(nodes->GetChildOrThrow(id)->AsMap()->GetChildValueOrThrow<TString>("state"), "Aborted");
    }
    for (const auto& id : {"1", "2", "3", "4"}) {
        EXPECT_TRUE(nodes->GetChildOrThrow(id)->AsMap()->FindChild("finishedAt"));
    }
}

TEST(TProgressMergerTest, RecordsFinishTimeForEveryTerminalState)
{
    for (const auto state : {EState::Finished, EState::Failed, EState::Aborted}) {
        for (const bool started : {false, true}) {
            TProgressMerger merger;
            if (started) {
                merger.MergeWith(NYql::TOperationProgress("yt", 1, EState::Started));
            }
            merger.MergeWith(NYql::TOperationProgress("yt", 1, state));
            auto node = ConvertToNode(NYson::TYsonString(merger.ToYsonString()))->AsMap()->GetChildOrThrow("1")->AsMap();
            auto finishedAt = node->GetChildValueOrThrow<TString>("finishedAt");

            merger.MergeWith(NYql::TOperationProgress("yt", 1, state));
            node = ConvertToNode(NYson::TYsonString(merger.ToYsonString()))->AsMap()->GetChildOrThrow("1")->AsMap();
            EXPECT_EQ(node->GetChildValueOrThrow<TString>("finishedAt"), finishedAt);
        }
    }
}

} // namespace
} // namespace NYT::NYqlPlugin
