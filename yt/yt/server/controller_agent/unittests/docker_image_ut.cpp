#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/controller_agent/controllers/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NControllerAgent::NControllers {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDockerImageSpecTest, TestDockerImageParser)
{
    auto defaultConfig = New<TDockerRegistryConfig>();
    auto internalConfig = New<TDockerRegistryConfig>();

    defaultConfig->Postprocess();

    internalConfig->InternalRegistryAddress = "internal.registry";
    internalConfig->InternalRegistryAlternativeAddresses.push_back("internal.registry:8080");
    internalConfig->InternalRegistryAlternativeAddresses.push_back("alt.internal.registry");
    internalConfig->InternalRegistryRegex = New<NRe2::TRe2>("registry\\.regex[0-9]");
    internalConfig->Postprocess();

    // Format: [REGISTRY[:PORT]/]IMAGE[:TAG][@DIGEST]

    {
        TDockerImageSpec image("image", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "image:latest");
    }

    {
        TDockerImageSpec image("image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "image:tag");
    }

    {
        TDockerImageSpec image("image@hash:digest", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "");
        EXPECT_EQ(image.Digest, "hash:digest");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "image@hash:digest");
    }

    {
        TDockerImageSpec image("image:tag@hash:digest", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "hash:digest");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "image:tag@hash:digest");
    }

    {
        TDockerImageSpec image("home/user/image", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "home/user/image:latest");
    }

    {
        TDockerImageSpec image("home/user/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "home/user/image:tag");
    }

    {
        TDockerImageSpec image("localhost:5000/image", defaultConfig);
        EXPECT_EQ(image.Registry, "localhost:5000");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "localhost:5000/image:latest");
    }

    {
        TDockerImageSpec image("localhost:5000/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "localhost:5000");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "localhost:5000/image:tag");
    }

    {
        TDockerImageSpec image("localhost:5000/image@hash:digest", defaultConfig);
        EXPECT_EQ(image.Registry, "localhost:5000");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "");
        EXPECT_EQ(image.Digest, "hash:digest");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "localhost:5000/image@hash:digest");
    }

    {
        TDockerImageSpec image("localhost:5000/image:tag@hash:digest", defaultConfig);
        EXPECT_EQ(image.Registry, "localhost:5000");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "hash:digest");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "localhost:5000/image:tag@hash:digest");
    }

    {
        TDockerImageSpec image("external.registry/image", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry/image:latest");
    }

    {
        TDockerImageSpec image("external.registry/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry/image:tag");
    }

    {
        TDockerImageSpec image("external.registry:8888/image", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry:8888");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry:8888/image:latest");
    }

    {
        TDockerImageSpec image("external.registry:8888/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "external.registry:8888");
        EXPECT_EQ(image.Image, "image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry:8888/image:tag");
    }

    {
        TDockerImageSpec image("internal.registry/project/image:tag", defaultConfig);
        EXPECT_EQ(image.Registry, "internal.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "internal.registry/project/image:tag");
    }

    {
        TDockerImageSpec image("external.registry/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry/project/image:tag");
    }

    {
        TDockerImageSpec image("external.registry/project/image:tag@hash:digest", internalConfig);
        EXPECT_EQ(image.Registry, "external.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "hash:digest");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "external.registry/project/image:tag@hash:digest");
    }

    {
        TDockerImageSpec image("home/user/image", internalConfig);
        EXPECT_EQ(image.Registry, "internal.registry");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "latest");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "internal.registry/home/user/image:latest");
    }

    {
        TDockerImageSpec image("home/user/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "internal.registry");
        EXPECT_EQ(image.Image, "home/user/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "internal.registry/home/user/image:tag");
    }

    {
        TDockerImageSpec image("internal.registry/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "internal.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "internal.registry/project/image:tag");
    }

    {
        TDockerImageSpec image("internal.registry:8080/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "internal.registry:8080");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "internal.registry:8080/project/image:tag");
    }

    {
        TDockerImageSpec image("alt.internal.registry/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "alt.internal.registry");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "alt.internal.registry/project/image:tag");
    }

    {
        TDockerImageSpec image("registry.regex1/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "registry.regex1");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "registry.regex1/project/image:tag");
    }

    {
        TDockerImageSpec image("registry.regex5/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "registry.regex5");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, true);
        EXPECT_EQ(image.GetDockerImage(), "registry.regex5/project/image:tag");
    }

    {
        TDockerImageSpec image("registry.regex123/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "registry.regex123");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "registry.regex123/project/image:tag");
    }

    {
        TDockerImageSpec image("prefix.registry.regex1/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "prefix.registry.regex1");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "prefix.registry.regex1/project/image:tag");
    }

    {
        TDockerImageSpec image("registry.regex1.suffix/project/image:tag", internalConfig);
        EXPECT_EQ(image.Registry, "registry.regex1.suffix");
        EXPECT_EQ(image.Image, "project/image");
        EXPECT_EQ(image.Tag, "tag");
        EXPECT_EQ(image.Digest, "");
        EXPECT_EQ(image.IsInternal, false);
        EXPECT_EQ(image.GetDockerImage(), "registry.regex1.suffix/project/image:tag");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent::NControllers
