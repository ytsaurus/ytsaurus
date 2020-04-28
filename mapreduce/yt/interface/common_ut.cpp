#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/serialize.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/generic/xrange.h>

#include <library/unittest/registar.h>

using namespace NYT;

template <typename T>
TString ToYson(const T& x)
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(x, &builder);
    return NodeToYsonString(result);
}

#define ASSERT_SERIALIZABLES_EQUAL(a, b) \
    UNIT_ASSERT_EQUAL_C(a, b, ToYson(a) << " != " << ToYson(b))

#define ASSERT_SERIALIZABLES_UNEQUAL(a, b) \
    UNIT_ASSERT_UNEQUAL_C(a, b, ToYson(a) << " == " << ToYson(b))

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
            for (auto i: xrange(columns.size())) {
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].Name(), columns[i]);
                UNIT_ASSERT_VALUES_EQUAL(schema.Columns()[i].SortOrder(), ESortOrder::SO_ASCENDING);
            }
            for (auto i: xrange(columns.size(), (size_t)initialSchema.Columns().size())) {
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

    Y_UNIT_TEST(TColumnSchema_TypeV3)
    {
        {
            auto column = TColumnSchema().Type(NTi::Interval());
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), true);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_INTERVAL);
        }
        {
            auto column = TColumnSchema().Type(NTi::Optional(NTi::Date()));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_DATE);
        }
        {
            auto column = TColumnSchema().Type(NTi::Null());
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_NULL);
        }
        {
            auto column = TColumnSchema().Type(NTi::Optional(NTi::Null()));
            UNIT_ASSERT_VALUES_EQUAL(column.Required(), false);
            UNIT_ASSERT_VALUES_EQUAL(column.Type(), VT_ANY);
        }
    }

    Y_UNIT_TEST(ToTypeV3)
    {
        UNIT_ASSERT_VALUES_EQUAL(*ToTypeV3(VT_INT32, true), *NTi::Int32());
        UNIT_ASSERT_VALUES_EQUAL(*ToTypeV3(VT_UTF8, false), *NTi::Optional(NTi::Utf8()));
    }

    Y_UNIT_TEST(DeserializeColumn)
    {
        auto deserialize = [] (TStringBuf yson) {
            auto node = NodeFromYsonString(yson);
            TColumnSchema column;
            Deserialize(column, node);
            return column;
        };

        auto column = deserialize("{name=foo; type=int64; required=%false}");
        UNIT_ASSERT_VALUES_EQUAL(column.Name(), "foo");
        UNIT_ASSERT_VALUES_EQUAL(*column.TypeV3(), *NTi::Optional(NTi::Int64()));

        column = deserialize("{name=bar; type=utf8; required=%true; type_v3=utf8}");
        UNIT_ASSERT_VALUES_EQUAL(column.Name(), "bar");
        UNIT_ASSERT_VALUES_EQUAL(*column.TypeV3(), *NTi::Utf8());
    }

    Y_UNIT_TEST(ColumnSchemaEquality)
    {
        auto base = TColumnSchema()
            .Name("col")
            .TypeV3(NTi::Optional(NTi::List(NTi::String())))
            .SortOrder(ESortOrder::SO_ASCENDING)
            .Lock("lock")
            .Expression("x + 12")
            .Aggregate("sum")
            .Group("group");

        auto other = base;
        ASSERT_SERIALIZABLES_EQUAL(other, base);
        other.Name("other");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.TypeV3(NTi::List(NTi::String()));
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.ResetSortOrder();
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Lock("lock1");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Expression("x + 13");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.ResetAggregate();
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);

        other = base;
        other.Group("group1");
        ASSERT_SERIALIZABLES_UNEQUAL(other, base);
    }

    Y_UNIT_TEST(TableSchemaEquality)
    {
        auto col1 = TColumnSchema()
            .Name("col1")
            .TypeV3(NTi::Optional(NTi::List(NTi::String())))
            .SortOrder(ESortOrder::SO_ASCENDING);

        auto col2 = TColumnSchema()
            .Name("col2")
            .TypeV3(NTi::Uint32());

        auto schema = TTableSchema()
            .AddColumn(col1)
            .AddColumn(col2)
            .Strict(true)
            .UniqueKeys(true);

        auto other = schema;
        ASSERT_SERIALIZABLES_EQUAL(other, schema);

        other.Strict(false);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.MutableColumns()[0].TypeV3(NTi::List(NTi::String()));
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.MutableColumns().push_back(col1);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);

        other = schema;
        other.UniqueKeys(false);
        ASSERT_SERIALIZABLES_UNEQUAL(other, schema);
    }
}
