#include <mapreduce/yt/interface/common.h>
#include <util/generic/xrange.h>

#include <library/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(Common)
{
    Y_UNIT_TEST(TKeyBaseColunms)
    {
        TKeyColumns keys1("a", "b");
        UNIT_ASSERT((keys1.Parts_ == TVector<TString>{"a", "b"}));

        keys1.Add("c", "d");
        UNIT_ASSERT((keys1.Parts_ == TVector<TString>{"a", "b", "c", "d"}));

        auto keys2 = TKeyColumns(keys1).Add("e", "f");
        UNIT_ASSERT((keys1.Parts_ == TVector<TString>{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys2.Parts_ == TVector<TString>{"a", "b", "c", "d", "e", "f"}));

        auto keys3 = TKeyColumns(keys1).Add("e").Add("f").Add("g");
        UNIT_ASSERT((keys1.Parts_ == TVector<TString>{"a", "b", "c", "d"}));
        UNIT_ASSERT((keys3.Parts_ == TVector<TString>{"a", "b", "c", "d", "e", "f", "g"}));
    }

    Y_UNIT_TEST(TTableSchema)
    {
        TTableSchema schema;
        schema
            .AddColumn(TColumnSchema().Name("a").Type(EValueType::VT_STRING).SortOrder(SO_ASCENDING))
            .AddColumn(TColumnSchema().Name("b").Type(EValueType::VT_UINT64))
            .AddColumn(TColumnSchema().Name("c").Type(EValueType::VT_INT64))
            ;
        auto checkSortBy = [](TTableSchema schema, const TVector<TString>& columns) {
            auto initialSchema = schema;
            schema.SortBy(columns);
            for (const auto& i: xrange(columns.size())) {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].Name(), columns[i]);
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].SortOrder(), ESortOrder::SO_ASCENDING);
            }
            for (const auto& i: xrange(columns.size(), (size_t)initialSchema.Columns().size())) {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].SortOrder(), Nothing());
            }
            UNIT_ASSERT_VALUES_EQUAL(initialSchema.Columns().size(), schema.Columns().size());
            return schema;
        };
        auto newSchema = checkSortBy(schema, {"b"});
        UNIT_ASSERT_VALUES_EQUAL(newSchema.Columns()[1].Name(), TString("a"));
        UNIT_ASSERT_VALUES_EQUAL(newSchema.Columns()[2].Name(), TString("c"));
        checkSortBy(schema, {"b", "c"});
        checkSortBy(schema, {"c", "a"});
        UNIT_ASSERT_EXCEPTION(checkSortBy(schema, {"b", "b"}), yexception);
        UNIT_ASSERT_EXCEPTION(checkSortBy(schema, {"a", "junk"}), yexception);
    }
}
