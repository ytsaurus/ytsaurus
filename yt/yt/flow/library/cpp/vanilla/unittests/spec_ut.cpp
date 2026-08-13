#include <yt/yt/flow/library/cpp/vanilla/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TVanillaSpec MakeSpec(TVanillaTaskSpec task)
{
    TVanillaSpec spec;
    task.Name = "worker";
    spec.Tasks.push_back(std::move(task));
    return spec;
}

IMapNodePtr GetTask(const IMapNodePtr& spec)
{
    return spec->GetChildOrThrow("tasks")->AsMap()->GetChildOrThrow("worker")->AsMap();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaSpecTest, EmitsDockerImage)
{
    TVanillaTaskSpec task;
    task.DockerImage = "registry.example.com/image:tag";

    auto operationSpec = BuildVanillaOperationSpec(MakeSpec(std::move(task)));

    EXPECT_EQ(
        "registry.example.com/image:tag",
        GetTask(operationSpec)->GetChildOrThrow("docker_image")->GetValue<std::string>());
    // A docker image is the counterpart of layers, so it must not pin the operation to porto nodes
    // the way UsePorto does.
    EXPECT_FALSE(operationSpec->FindChild("scheduling_tag_filter"));
}

TEST(TVanillaSpecTest, OmitsDockerImageWhenUnset)
{
    auto operationSpec = BuildVanillaOperationSpec(MakeSpec(TVanillaTaskSpec()));

    EXPECT_FALSE(GetTask(operationSpec)->FindChild("docker_image"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
