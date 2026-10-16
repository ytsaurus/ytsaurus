#include <yt/yt/flow/library/cpp/connectors/sorted_dynamic_table/spec.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NSortedDynamicTable {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncSinkParametersTest, RejectsNonEmptyAggregateColumns)
{
    EXPECT_NO_THROW(ConvertTo<TAsyncSinkParametersPtr>(TYsonString(TStringBuf(R"({
        table_path = "//tmp/t";
    })"))));

    EXPECT_NO_THROW(ConvertTo<TAsyncSinkParametersPtr>(TYsonString(TStringBuf(R"({
        table_path = "//tmp/t";
        aggregate_columns = [];
    })"))));

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TAsyncSinkParametersPtr>(TYsonString(TStringBuf(R"({
            table_path = "//tmp/t";
            aggregate_columns = [count];
        })"))),
        "aggregate_columns");
}

TEST(TDynamicAsyncSinkParametersTest, RejectsInvalidParameters)
{
    EXPECT_NO_THROW(ConvertTo<TDynamicAsyncSinkParametersPtr>(TYsonString(TStringBuf("{ }"))));

    EXPECT_THROW(
        ConvertTo<TDynamicAsyncSinkParametersPtr>(TYsonString(TStringBuf("{ backoff_duration = 0; }"))),
        std::exception);

    EXPECT_THROW(
        ConvertTo<TDynamicAsyncSinkParametersPtr>(TYsonString(TStringBuf("{ transaction_timeout = 15s; }"))),
        std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NSortedDynamicTable
