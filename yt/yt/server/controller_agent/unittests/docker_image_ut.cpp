#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/controller_agent/controllers/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NControllerAgent::NControllers {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDockerImageSpecTest, TestDockerImageParser)
{
    auto defaultConfig = New<TDockerRegistryConfig>();
    auto internalConfig = New<TDockerRegistryConfig>();

    internalConfig->InternalRegistryAddress = "internal.registry";

    // Format: [REGISTRY[:PORT]/]IMAGE[:TAG]

    {
        TDockerImageSpec image("image", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.IsInternal(), true);
    }

    {
        TDockerImageSpec image("home/user/image", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.IsInternal(), true);
    }

    {
        TDockerImageSpec image("home/user/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), true);
    }

    {
        TDockerImageSpec image("external.registry/image", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("external.registry/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("external.registry:8888/image", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry:8888");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("external.registry:8888/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry:8888");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("internal.registry/project/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "internal.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("external.registry/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), false);
    }

    {
        TDockerImageSpec image("internal.registry/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.IsInternal(), true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent::NControllers
