#pragma once

#include <yt/yt/library/query/base/callbacks.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/engine/functions_builder.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/test_framework/framework.h>

#define _MIN_ "<\"type\"=\"min\">#"
#define _MAX_ "<\"type\"=\"max\">#"
#define _NULL_ "#"

namespace NYT::NQueryClient {

using ::testing::_;
using ::testing::StrictMock;
using ::testing::NiceMock;
using ::testing::HasSubstr;
using ::testing::ContainsRegex;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::AllOf;

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os);

TValue MakeInt64(i64 value);
TValue MakeUint64(ui64 value);
TValue MakeDouble(double value);
TValue MakeBoolean(bool value);
TValue MakeString(TStringBuf value);
TValue MakeNull();

template <class TTypedExpression, class... TArgs>
TConstExpressionPtr Make(TArgs&&... args)
{
    if constexpr (std::is_same_v<TTypedExpression, TReferenceExpression>) {
        return New<TTypedExpression>(
            SimpleLogicalType(ESimpleLogicalValueType::Null),
            std::forward<TArgs>(args)...);
    } else {
        return New<TTypedExpression>(
            EValueType::Null,
            std::forward<TArgs>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPrepareCallbacksMock
    : public IPrepareCallbacks
{
public:
    MOCK_METHOD(TFuture<TDataSplit>, GetInitialSplit, (const TYPath&), (override));
};

TKeyColumns GetSampleKeyColumns();
TKeyColumns GetSampleKeyColumns2();
TTableSchemaPtr GetSampleTableSchema();

TDataSplit MakeSimpleSplit(const TYPath& path, ui64 counter = 0);
TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns, ui64 counter = 0);

TFuture<TDataSplit> RaiseTableNotFound(const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

void ProfileForBothExecutionBackends(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler);

void ProfileForBothExecutionBackends(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables);

////////////////////////////////////////////////////////////////////////////////

bool EnableWebAssemblyInUnitTests();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
