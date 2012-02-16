#include "stdafx.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/ephemeral.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathTest: public ::testing::Test
{
public:
    TYPathServicePtr RootService;

    virtual void SetUp()
    {
        RootService = GetEphemeralNodeFactory()->CreateMap();
    }

    static TYson TextifyYson(const TYson& data)
    {
        TStringStream outputStream;
        TYsonWriter writer(&outputStream, EYsonFormat::Text);
        TStringInput input(data);
        TYsonReader reader(&writer, &input);
        reader.Read();
        return outputStream.Str();
    }

    void Set(const TYPath& path, const TYson& value)
    {
        SyncYPathSet(~RootService, path, value);
    }

    void Remove(const TYPath& path)
    {
        SyncYPathRemove(~RootService, path);
    }

    TYson Get(const TYPath& path)
    {
        return TextifyYson(SyncYPathGet(~RootService, path));
    }

    yvector<Stroka> List(const TYPath& path)
    {
        return SyncYPathList(~RootService, path);
    }

    void Check(const TYPath& path, TYson expected)
    {
        TYson output = Get(path);
//        Cout << Endl;
//        Cout << "output:   " << output << Endl;
//        Cout << "expected: " << expected << Endl;
        EXPECT_EQ(expected, output);
    }

    void CheckList(const TYPath& path, TYson expected)
    {
        VectorStrok result;
        SplitStroku(&result, expected, ";");

        for (int index = 0; index < result.ysize(); ++index) {
            Check(path + "/" + ToString(index), result[index]);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYPathTest, MapModification)
{
    Set("map", "{\"hello\"=\"world\"; \"list\"=[0;\"a\";{}]; \"n\"=1}");

    Set("map/hello", "not_world");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[0;\"a\";{}];\"n\"=1}}");

    Set("map/list/2/some", "value");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[0;\"a\";{\"some\"=\"value\"}];\"n\"=1}}");

    Remove("map/n");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[0;\"a\";{\"some\"=\"value\"}]}}");

    Set("map/list", "[]");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[]}}");

    Set("map/list/+/a", "1");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[{\"a\"=1}]}}");

    Set("map/list/-/b", "2");
    Check("", "{\"map\"={\"hello\"=\"not_world\";\"list\"=[{\"b\"=2};{\"a\"=1}]}}");

    Remove("map/hello");
    Check("", "{\"map\"={\"list\"=[{\"b\"=2};{\"a\"=1}]}}");

    Remove("map");
    Check("", "{}");
}

TEST_F(TYPathTest, ListModification)
{
    Set("list", "[1;2;3]");

    Set("list/+", "100");
    Check("", "{\"list\"=[1;2;3;100]}");

    Set("list/-", "200");
    Check("", "{\"list\"=[200;1;2;3;100]}");

    Set("list/-", "500");
    Check("", "{\"list\"=[500;200;1;2;3;100]}");

    Set("list/2+", "1000");
    Check("", "{\"list\"=[500;200;1;1000;2;3;100]}");

    Set("list/3", "220");
    Check("", "{\"list\"=[500;200;1;220;2;3;100]}");
    Check("list/3", "220");

    Remove("list/4");
    Check("", "{\"list\"=[500;200;1;220;3;100]}");
    CheckList("list", "500;200;1;220;3;100");

    Remove("list/4");
    Check("", "{\"list\"=[500;200;1;220;100]}");
    CheckList("list", "500;200;1;220;100");

    Remove("list/0");
    Check("", "{\"list\"=[200;1;220;100]}");
    CheckList("list", "200;1;220;100");

    Set("list/+", "666");
    Check("", "{\"list\"=[200;1;220;100;666]}");
    CheckList("list", "200;1;220;100;666");

    Set("list/-", "777");
    Check("", "{\"list\"=[777;200;1;220;100;666]}");
    CheckList("list", "777;200;1;220;100;666");
}

TEST_F(TYPathTest, ListReassignment)
{
    Set("list", "[a;b;c]");
    Set("list", "[1;2;3]");

    Check("", "{\"list\"=[1;2;3]}");
}

TEST_F(TYPathTest, Ls)
{
    Set("d/x4/y4", "4");
    Set("c/x3/y3", "3");
    Set("b/x2/y2", "2");
    Set("a/x1/y1", "1");

    Remove("b");
    Set("e", "5");

    auto result = List("");
    std::sort(result.begin(), result.end());

    yvector<Stroka> expected;
    expected.push_back("a");
    expected.push_back("c");
    expected.push_back("d");
    expected.push_back("e");

    EXPECT_EQ(expected, result);
}

TEST_F(TYPathTest, Attributes)
{
    Set("root", "{nodes=[\"1\"; \"2\"]} <attr=100;mode=\"rw\">");
    Check("root@", "{\"attr\"=100;\"mode\"=\"rw\"}");
    Check("root@attr", "100");

    Remove("root@");
    Check("root@", "{}");

    Remove("root/nodes");
    Check("", "{\"root\"={}}");

    Set("root/2", "<author=\"ignat\">");
    Check("", "{\"root\"={\"2\"=<>}}");
    Check("root/2@", "{\"author\"=\"ignat\"}");
    Check("root/2@author", "\"ignat\"");

    Set("root/3", "<dir=<file=-100<>>>");
    Check("root/3@", "{\"dir\"=<\"file\"=-100>}");
    Check("root/3@dir@", "{\"file\"=-100}");
    Check("root/3@dir@file", "-100");
    Check("root/3@dir@file@", "{}");
}

TEST_F(TYPathTest, InvalidCases)
{
    // empty path
    EXPECT_ANY_THROW(Set("/a", "{}"));

    // change the type of root
    EXPECT_ANY_THROW(Set("", "[]"));

    // remove the root
    EXPECT_ANY_THROW(Remove(""));

    // remove non-existing child
    EXPECT_ANY_THROW(Remove("a"));

//    EXPECT_ANY_THROW(Set("/@/some", "{}"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
