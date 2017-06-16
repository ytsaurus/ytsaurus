#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/ypath_resolver.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

#define EXPECT_NULL(NULLABLE) \
    EXPECT_FALSE(NULLABLE.HasValue())

#define EXPECT_VALUE(VALUE, NULLABLE) \
    do { \
        EXPECT_TRUE(NULLABLE.HasValue()); \
        if (NULLABLE.HasValue()) { \
            EXPECT_EQ(VALUE, NULLABLE.Get()); \
        } \
    } while (false)

TEST(TYPathResolver, GetInt64)
{
    EXPECT_VALUE(2, TryGetInt64("{key3=2;key4=5}", "/key3"));
    EXPECT_VALUE(5, TryGetInt64("{key3=2;key4=5}", "/key4"));
    EXPECT_NULL(TryGetInt64("{key3=2;key4=5}", "/key6"));
    EXPECT_VALUE(5, TryGetInt64("5", ""));
    EXPECT_VALUE(10, TryGetInt64("{key3={k=2;a={b=3}};k={k2=3;k3=10}}", "/k/k3"));
    EXPECT_VALUE(2, TryGetInt64("{l=[0;1];key3=2;k={k2=3;k3=10}}", "/key3"));
    EXPECT_VALUE(3, TryGetInt64("{key3=2;k={k2=3;k3=10}}", "/k/k2"));
    EXPECT_VALUE(1, TryGetInt64("{key3=2;l=[1;2];k={k2=3;k3=10};lst=[0;1;2]}", "/lst/1"));
    EXPECT_VALUE(4, TryGetInt64("<attr=4>{key3=2;k={k2=3;k3=10};lst=[0;1;2]}", "/@attr"));
    EXPECT_VALUE(7, TryGetInt64("<attr=4>{key3=2;k=<a=6>{k2=<a=7>3;k3=10};lst=[0;1;2]}", "/k/k2/@a"));
    EXPECT_VALUE(4, TryGetInt64("<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=[0;1;<a={b=4}>2]}", "/lst/2/@a/b"));
    EXPECT_VALUE(7, TryGetInt64("<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7}]>[0;1;<a={b=4}>2]}", "/lst/@a/2/b"));
    EXPECT_NULL(TryGetInt64("<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7}]>[0;1;<a={b=4}>2]}", "/@a"));
    EXPECT_NULL(TryGetInt64("<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7}]>[0;1;<a={b=4}>2]}", "/key3/k"));
}

TEST(TYPathResolver, GetUint64)
{
    EXPECT_VALUE(2, TryGetUint64("{key3=2u;key4=5}", "/key3"));
    EXPECT_NULL(TryGetUint64("{key3=2;key4=5}", "/key3"));
    EXPECT_VALUE(5, TryGetUint64("{key3=2;key4=5u}", "/key4"));
    EXPECT_NULL(TryGetUint64("{key3=2;key4=5}", "/key6"));
    EXPECT_VALUE(7, TryGetUint64("<attr=4>{key3=2;k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7u}]>[0;1;<a={b=4}>2]}", "/lst/@a/2/b"));
    EXPECT_NULL(TryGetUint64("<attr=4>{key3=\"2\";k={k2=<b=7>3;k3=10};lst=<a=[1;{a=3};{b=7}]>[0;1;<a={b=4}>2]}", "/key3"));
}

TEST(TYPathResolver, GetBoolean)
{
    EXPECT_VALUE(true, TryGetBoolean("{key3=2;key4=%true}", "/key4"));
    EXPECT_VALUE(false, TryGetBoolean("{key3=%false;key4=%true}", "/key3"));
    EXPECT_NULL(TryGetBoolean("{key3=%false;key4=%true}", "/key5"));
}

TEST(TYPathResolver, GetDouble)
{
    EXPECT_VALUE(7., TryGetDouble("{key3=2;key4=7.}", "/key4"));
    EXPECT_NULL(TryGetDouble("{key3=2;key4=7.}", "/key3"));
    EXPECT_NULL(TryGetDouble("{key3=2;key4=7.}", "/key2"));
}

TEST(TYPathResolver, GetString)
{
    EXPECT_VALUE("s", TryGetString("{key3=2;key4=\"s\"}", "/key4"));
    EXPECT_NULL(TryGetString("{key3=2;key4=\"s\"}", "/key3"));
    EXPECT_NULL(TryGetString("{key3=2;key4=\"s\"}", "/key2"));
}

TEST(TYPathResolver, InvalidYPath)
{
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "//"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "/key3/"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "/@@"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "@"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "/"), std::exception);
    EXPECT_THROW(TryGetInt64("<x=2>{key3=2;key4=5}", "/@x/"), std::exception);
    EXPECT_THROW(TryGetInt64("<x=2>{key3=2;key4=5}", "/@x//"), std::exception);
    EXPECT_THROW(TryGetInt64("<x={sdf=2}>{key3=2;key4=5}", "/@x/sdf@"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "dfsdf"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5}", "@dfsdf"), std::exception);
}

TEST(TYPathResolver, InvalidYson)
{
    EXPECT_THROW(TryGetInt64("{key3=2;key4=5", "/k"), std::exception);
    EXPECT_THROW(TryGetInt64("{key3=2key4=5}", "/k"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
