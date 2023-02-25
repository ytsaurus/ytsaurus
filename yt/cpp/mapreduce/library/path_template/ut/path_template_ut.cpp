#include <yt/cpp/mapreduce/library/path_template/path_template.h>
#include <yt/cpp/mapreduce/library/path_template/path_template_safe.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYtPathTemplate;

Y_UNIT_TEST_SUITE(PathTemplate) {
    Y_UNIT_TEST(PathTemplate) {
        const auto root = MakeRootNode("//WorkingRoot");

        const auto subPath = MakeChildNode(root, "/SubPath");
        const auto table1 = MakeChildNode(subPath, "/Table1");
        const auto file1 = MakeChildNode(subPath, "/File1");

        const auto states = MakeChildNode(root, "/States");
        const auto table2 = MakeChildNode(states, "/Table2");

        const auto someState = MakeChildNode(states);
        const auto table3 = MakeChildNode(someState, "/Table3");
        const auto table4 = MakeChildNode(someState, "/Table4");

        UNIT_ASSERT_VALUES_EQUAL(root->ToPathUnsafe(), "//WorkingRoot");

        UNIT_ASSERT_VALUES_EQUAL(subPath->ToPathUnsafe(), "//WorkingRoot/SubPath");
        UNIT_ASSERT_VALUES_EQUAL(table1->ToPathUnsafe(), "//WorkingRoot/SubPath/Table1");
        UNIT_ASSERT_VALUES_EQUAL(file1->ToPathUnsafe(), "//WorkingRoot/SubPath/File1");

        UNIT_ASSERT_VALUES_EQUAL(states->ToPathUnsafe(), "//WorkingRoot/States");
        UNIT_ASSERT_VALUES_EQUAL(table2->ToPathUnsafe(), "//WorkingRoot/States/Table2");

        UNIT_ASSERT_VALUES_EQUAL(table3->ToPathUnsafe("/id12"), "//WorkingRoot/States/id12/Table3");
        UNIT_ASSERT_VALUES_EQUAL(table4->ToPathUnsafe("/id12"), "//WorkingRoot/States/id12/Table4");
    }

    Y_UNIT_TEST(PathTemplateEmptyNodesCount) {
        const auto root = MakeSafeRootNode("//WorkingRoot");

        const auto subPath = MakeSafeChildNode(root, "/SubPath");
        const auto table1 = MakeSafeChildNode(subPath, "/Table1");
        const auto file1 = MakeSafeChildNode(subPath, "/File1");

        const auto states = MakeSafeChildNode(root, "/States");
        const auto table2 = MakeSafeChildNode(states, "/Table2");

        const auto someState = MakeSafeChildNode(states);
        const auto table3 = MakeSafeChildNode(someState, "/Table3");
        const auto table4 = MakeSafeChildNode(someState, "/Table4");

        UNIT_ASSERT_VALUES_EQUAL(root->ToPathSafe(), "//WorkingRoot");

        UNIT_ASSERT_VALUES_EQUAL(subPath->ToPathSafe(), "//WorkingRoot/SubPath");
        UNIT_ASSERT_VALUES_EQUAL(table1->ToPathSafe(), "//WorkingRoot/SubPath/Table1");
        UNIT_ASSERT_VALUES_EQUAL(file1->ToPathSafe(), "//WorkingRoot/SubPath/File1");

        UNIT_ASSERT_VALUES_EQUAL(states->ToPathSafe(), "//WorkingRoot/States");
        UNIT_ASSERT_VALUES_EQUAL(table2->ToPathSafe(), "//WorkingRoot/States/Table2");

        UNIT_ASSERT_VALUES_EQUAL(table3->ToPathSafe("/id12"), "//WorkingRoot/States/id12/Table3");
        UNIT_ASSERT_VALUES_EQUAL(table4->ToPathSafe("/id12"), "//WorkingRoot/States/id12/Table4");
    }

    Y_UNIT_TEST(MultipleEmptyPathsUnsafe) {
        const auto root = MakeRootNode();
        const auto child1 = MakeChildNode(root);
        const auto child2 = MakeChildNode(child1);
        const auto child3 = MakeChildNode(child2);

        UNIT_ASSERT_VALUES_EQUAL(
            child3->ToPathUnsafe("//home", "/tmp", "/user", "/table1"),
            "//home/tmp/user/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            child3->ToPathUnsafe("//usr", "/home", "/yt", "/table2"),
            "//usr/home/yt/table2");
    }

    Y_UNIT_TEST(MultipleEmptyPathsSafe) {
        const auto root = MakeSafeRootNode();
        const auto child1 = MakeSafeChildNode(root);
        const auto child2 = MakeSafeChildNode(child1);
        const auto child3 = MakeSafeChildNode(child2);

        UNIT_ASSERT_VALUES_EQUAL(
            child3->ToPathSafe("//home", "/tmp", "/user", "/table1"),
            "//home/tmp/user/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            child3->ToPathSafe("//usr", "/home", "/yt", "/table2"),
            "//usr/home/yt/table2");
    }

    Y_UNIT_TEST(UnsafeRelativePaths) {
        const auto root = MakeRootNode();
        const auto subpath1 = MakeChildNode(root);
        const auto subpath2 = MakeChildNode(subpath1);
        const auto subpath3 = MakeChildNode(subpath2);
        const auto table1 = MakeChildNode(subpath3, "/table1");

        UNIT_ASSERT_VALUES_EQUAL(subpath3->UnsafePathRelativeTo(subpath2, "/home"), "/home");
        UNIT_ASSERT_VALUES_EQUAL(subpath3->UnsafePathRelativeTo(subpath1, "/home", "/tmp"), "/home/tmp");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->UnsafePathRelativeTo(subpath1, "/home", "/tmp"),
            "/home/tmp/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->UnsafePathRelativeTo(root, "//home", "/tmp", "/tables"),
            "//home/tmp/tables/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->ToPathUnsafe("//home", "/tmp", "/tables", "/tables"),
            "//home/tmp/tables/tables/table1");
    }

    Y_UNIT_TEST(SafeRelativePaths) {
        const auto root = MakeSafeRootNode();
        const auto subpath1 = MakeSafeChildNode(root);
        const auto subpath2 = MakeSafeChildNode(subpath1);
        const auto subpath3 = MakeSafeChildNode(subpath2);
        const auto table1 = MakeSafeChildNode(subpath3, "/table1");

        UNIT_ASSERT_VALUES_EQUAL(subpath3->SafePathRelativeTo(subpath2, "/home"), "/home");
        UNIT_ASSERT_VALUES_EQUAL(subpath3->SafePathRelativeTo(subpath1, "/home", "/tmp"), "/home/tmp");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->SafePathRelativeTo(subpath1, "/home", "/tmp"),
            "/home/tmp/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->SafePathRelativeTo(root, "//home", "/tmp", "/tables"),
            "//home/tmp/tables/table1");
        UNIT_ASSERT_VALUES_EQUAL(
            table1->ToPathSafe("//home", "/tmp", "/tables", "/tables"),
            "//home/tmp/tables/tables/table1");
    }
}
