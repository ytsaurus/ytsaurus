#include "stdafx.h"

#include "../ytlib/ytree/ypath_detail.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TEST(TCombinePathsTest, SlashSlash)
{
    TYPath result = CombineYPaths("/", "/some/value");
    EXPECT_EQ("/some/value", result);
}

TEST(TCombinePathsTest, NoneSlash)
{
    TYPath result = CombineYPaths("root", "/some/value");
    EXPECT_EQ("root/some/value", result);
}

TEST(TCombinePathsTest, SlashNone)
{
    TYPath result = CombineYPaths("/root/", "some/value");
    EXPECT_EQ("/root/some/value", result);
}

TEST(TCombinePathsTest, NoneNone)
{
    TYPath result = CombineYPaths("/root", "some/value");
    EXPECT_EQ("/root/some/value", result);
}

TEST(TCombinePathsTest, AnyEmpty)
{
    TYPath result = CombineYPaths("/", "");
    EXPECT_EQ("/", result);
}

TEST(TCombinePathsTest, EmptyAny)
{
    TYPath result = CombineYPaths("", "/");
    EXPECT_EQ("/", result);
}

TEST(TCombinePathsTest, EmptyEmpty)
{
    TYPath result = CombineYPaths("", "");
    EXPECT_EQ("", result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
