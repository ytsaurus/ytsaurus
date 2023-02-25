#include <yt/cpp/mapreduce/library/cypress_path/cypress_path.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT::NTesting;
using NYT::TCypressPath;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCypressPathTestSuite) {

    Y_UNIT_TEST(TestValidate)
    {
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("//"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("folder"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("/**"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("///path"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("/path/"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("path/child"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("//home/petya/data[#10:#20]"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("///"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("/*/home"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("/@home@"), NYT::TErrorException);
        UNIT_ASSERT_EXCEPTION(TCypressPath::Validate("//*/x"), NYT::TErrorException);

        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate(""));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("/path"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("//path"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("#1-2-a-b"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("#1-2-a-b/child"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("/"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("/*"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("#1-1-1-1/child/other/child"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("#0-1-c-d/child/other/child&"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("#0-1-c-d/child/other/@child"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("//child/other/@child/*"));
        UNIT_ASSERT_NO_EXCEPTION(TCypressPath::Validate("//some/list/after:5"));
    }

    Y_UNIT_TEST(TestUnion)
    {
        TCypressPath path(TString("/home"));
        UNIT_ASSERT_VALUES_EQUAL(path / "table", TCypressPath("/home/table"));
        path /= "table";
        UNIT_ASSERT_VALUES_EQUAL(path, TCypressPath("/home/table"));

        TCypressPath root(TString("/"));
        UNIT_ASSERT_VALUES_EQUAL(root / "/home/table", TCypressPath("//home/table"));
        UNIT_ASSERT_VALUES_EQUAL(root / path, TCypressPath("//home/table"));
        root /= path;
        UNIT_ASSERT_VALUES_EQUAL(root, TCypressPath("//home/table"));

        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-2-a-b") / "/child", TCypressPath("#1-2-a-b/child"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/") / "/home/child/folder&", TCypressPath("//home/child/folder&"));

        UNIT_ASSERT_EXCEPTION(path / root, NYT::TErrorException);
    }

    Y_UNIT_TEST(TestPathMethods)
    {
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/").IsAbsolute(), true);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/").IsRelative(), false);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-1-c-1/file").IsAbsolute(), true);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-1-c-1/file").IsRelative(), false);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/home/file").IsRelative(), true);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/home/file").IsAbsolute(), false);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("//home/file").IsAbsolute(), true);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("//home/file").IsRelative(), false);
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("").IsRelative(), true);

        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("").GetBasename(), TCypressPath(""));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("").GetParent(), TCypressPath(""));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/").GetBasename(), TCypressPath("/"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/").GetParent(), TCypressPath("/"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-2-3-4").GetBasename(), TCypressPath("#1-2-3-4"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-2-3-4").GetParent(), TCypressPath("#1-2-3-4"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-2-3-4/child/other/child").GetBasename(), TCypressPath("/child"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("#1-2-3-4/child/other").GetParent(), TCypressPath("#1-2-3-4/child"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/home/child/other/@child").GetBasename(), TCypressPath("/@child"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("/home/child/other/@child").GetParent(), TCypressPath("/home/child/other"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("//home").GetBasename(), TCypressPath("/home"));
        UNIT_ASSERT_VALUES_EQUAL(TCypressPath("//home").GetParent(), TCypressPath("/"));
    }
}

////////////////////////////////////////////////////////////////////////////////
