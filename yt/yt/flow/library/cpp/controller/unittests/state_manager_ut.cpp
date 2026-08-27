#include <yt/yt/flow/library/cpp/controller/state_manager.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStateManagerTest, ReclaimsOrphanedState)
{
    auto remoteState = New<TJobManagerState>();
    auto manager = New<TStateManager>(remoteState);
    TComputationId computationId("computation");

    TMutableStateClient<std::string> keep;
    manager->CreateContext(computationId)->InitClient(keep, "keep");
    *keep = "kept";

    {
        TMutableStateClient<std::string> drop;
        manager->CreateContext(computationId)->InitClient(drop, "drop");
        *drop = "dropped";

        manager->Sync();

        EXPECT_TRUE(remoteState->Computations[computationId].contains("/keep"));
        EXPECT_TRUE(remoteState->Computations[computationId].contains("/drop"));
    }

    // The "drop" holder is gone; the next sync must reclaim its persisted blob and keep "keep".
    manager->Sync();

    EXPECT_TRUE(remoteState->Computations[computationId].contains("/keep"));
    EXPECT_FALSE(remoteState->Computations[computationId].contains("/drop"));
}

TEST(TStateManagerTest, ReclaimsRemovedComputation)
{
    auto remoteState = New<TJobManagerState>();
    auto manager = New<TStateManager>(remoteState);
    TComputationId computationId("gone");

    {
        TMutableStateClient<std::string> state;
        manager->CreateContext(computationId)->InitClient(state, "state");
        *state = "value";

        manager->Sync();

        EXPECT_TRUE(remoteState->Computations.contains(computationId));
    }

    // The whole computation is gone now; its entry must be dropped entirely.
    manager->Sync();

    EXPECT_FALSE(remoteState->Computations.contains(computationId));
}

TEST(TStateManagerTest, SeparatesResourceControllerStatePrefixes)
{
    auto remoteState = New<TJobManagerState>();
    auto manager = New<TStateManager>(remoteState);
    TResourceId resourceId("resource");

    TMutableStateClient<std::string> resourceState;
    manager->CreateResourceContext(resourceId)
        ->WithPrefix("controller")
        ->InitClient(resourceState, "v0");
    *resourceState = "resource-controller";

    TMutableStateClient<std::string> fileSourceState;
    manager->CreateResourceContext(resourceId)
        ->WithPrefix("file_sources")
        ->InitClient(fileSourceState, "v0");
    *fileSourceState = "file-source-controller";

    manager->Sync();

    const auto& resourceDomain = remoteState->Computations.at(TComputationId("resource:resource"));
    EXPECT_TRUE(resourceDomain.contains("/controller/v0"));
    EXPECT_TRUE(resourceDomain.contains("/file_sources/v0"));
    EXPECT_EQ(NYTree::ConvertTo<std::string>(resourceDomain.at("/controller/v0")), "resource-controller");
    EXPECT_EQ(
        NYTree::ConvertTo<std::string>(resourceDomain.at("/file_sources/v0")),
        "file-source-controller");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
