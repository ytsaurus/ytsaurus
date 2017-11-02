#include <mapreduce/yt/interface/common.h>

#include <library/unittest/registar.h>

using namespace NYT;

SIMPLE_UNIT_TEST_SUITE(Common)
{
    SIMPLE_UNIT_TEST(TKeyBaseColunms)
    {
        TKeyColumns keys1("a", "b");
        UNIT_ASSERT((keys1.Parts_ == yvector<TString>{"a", "b"}));

        keys1.Add("c", "d");
        UNIT_ASSERT((keys1.Parts_ == yvector<TString>{"a", "b", "c", "d"}));

        auto keys2 = TKeyColumns(keys1).Add("e", "f");
        UNIT_ASSERT((keys1.Parts_ == yvector<TString>{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys2.Parts_ == yvector<TString>{"a", "b", "c", "d", "e", "f"}));

        auto keys3 = TKeyColumns(keys1).Add("e").Add("f").Add("g");
        UNIT_ASSERT((keys1.Parts_ == yvector<TString>{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys3.Parts_ == yvector<TString>{"a", "b", "c", "d", "e", "f", "g"}));
    }
}
