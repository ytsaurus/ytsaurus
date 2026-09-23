#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/containers/porto_helpers.h>

namespace NYT::NContainers {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TPortoContainerNameTest, ValidNameComponents)
{
    for (auto component : {
        "sidecar1",
        "my-sidecar",
        "a_b",
        "foo.bar",
        "foo@v1",
        "a:b",
        "...",
        "self",
        "",
        "..foo",
        "foo..",
        ".foo",
        "bad name",
    }) {
        EXPECT_TRUE(CheckPortoContainerNameComponent(component).IsOK())
            << "component: " << component;
    }
}

TEST(TPortoContainerNameTest, InvalidNameComponents)
{
    for (auto component : {
        "..",
        ".",
        "../container",
        "container/..",
        "a/../b",
        "a/./b",
        "/",
        "/foo",
        "foo/",
        "foo/bar",
    }) {
        EXPECT_FALSE(CheckPortoContainerNameComponent(component).IsOK())
            << "component: " << component;
    }
}

TEST(TPortoContainerNameTest, ValidSubpaths)
{
    for (auto subpath : {
        "",
        "/N",
        "/N/task",
        "/a/b/c",
        "/N.1",
        "/...",
    }) {
        EXPECT_TRUE(CheckPortoContainerSubpath(subpath).IsOK())
            << "subpath: " << subpath;
    }
}

TEST(TPortoContainerNameTest, InvalidSubpaths)
{
    for (auto subpath : {
        "/..",
        "/../..",
        "/N/../../js",
        "/../../container",
        "/N/.",
        "/.",
        "N",
        "-evil",
    }) {
        EXPECT_FALSE(CheckPortoContainerSubpath(subpath).IsOK())
            << "subpath: " << subpath;
    }
}

TEST(TPortoContainerNameTest, Validators)
{
    EXPECT_NO_THROW(ValidatePortoContainerNameComponent("sidecar1"));
    EXPECT_THROW(ValidatePortoContainerNameComponent("../container"), std::exception);

    EXPECT_NO_THROW(ValidatePortoContainerSubpath(""));
    EXPECT_NO_THROW(ValidatePortoContainerSubpath("/N"));
    EXPECT_THROW(ValidatePortoContainerSubpath("/../../js"), std::exception);
    EXPECT_THROW(ValidatePortoContainerSubpath("N"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers
