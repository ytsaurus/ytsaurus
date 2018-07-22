#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/client/table_client/helpers.h>

#define _MIN_ "<\"type\"=\"min\">#"
#define _MAX_ "<\"type\"=\"max\">#"
#define _NULL_ "#"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TOwningKey& key, ::std::ostream* os);
void PrintTo(const TUnversionedValue& value, ::std::ostream* os);
void PrintTo(const TUnversionedRow& value, ::std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

namespace NYT {
namespace NQueryClient {

using ::testing::_;
using ::testing::StrictMock;
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
TValue MakeUint64(i64 value);
TValue MakeDouble(i64 value);
TValue MakeBoolean(bool value);
TValue MakeString(TStringBuf value);
TValue MakeNull();

template <class TTypedExpression, class... TArgs>
TConstExpressionPtr Make(TArgs&&... args)
{
    return New<TTypedExpression>(
        EValueType::TheBottom,
        std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

class TPrepareCallbacksMock
    : public IPrepareCallbacks
{
public:
    MOCK_METHOD2(GetInitialSplit, TFuture<TDataSplit>(
        const TYPath&,
        TTimestamp));
};

MATCHER_P(HasCounter, expectedCounter, "")
{
    auto objectId = GetObjectIdFromDataSplit(arg);
    auto cellTag = CellTagFromId(objectId);
    auto counter = CounterFromId(objectId);

    if (cellTag != 0x42) {
        *result_listener << "cell id is bad";
        return false;
    }

    if (counter != expectedCounter) {
        *result_listener
            << "actual counter id is " << counter << " while "
            << "expected counter id is " << expectedCounter;
        return false;
    }

    return true;
}

MATCHER_P(HasSplitsCount, expectedCount, "")
{
    if (arg.size() != expectedCount) {
        *result_listener
            << "actual splits count is " << arg.size() << " while "
            << "expected count is " << expectedCount;
        return false;
    }

    return true;
}

MATCHER_P(HasLowerBound, encodedLowerBound, "")
{
    auto expected = NTableClient::YsonToKey(encodedLowerBound);
    auto actual = GetLowerBoundFromDataSplit(arg);

    auto result = CompareRows(expected, actual);

    if (result != 0 && result_listener->IsInterested()) {
        *result_listener << "expected lower bound to be ";
        PrintTo(expected, result_listener->stream());
        *result_listener << " while actual is ";
        PrintTo(actual, result_listener->stream());
        *result_listener
            << " which is "
            << (result > 0 ? "greater" : "lesser")
            << " than expected";
    }

    return result == 0;
}

MATCHER_P(HasUpperBound, encodedUpperBound, "")
{
    auto expected = NTableClient::YsonToKey(encodedUpperBound);
    auto actual = GetUpperBoundFromDataSplit(arg);

    auto result = CompareRows(expected, actual);

    if (result != 0) {
        *result_listener << "expected upper bound to be ";
        PrintTo(expected, result_listener->stream());
        *result_listener << " while actual is ";
        PrintTo(actual, result_listener->stream());
        *result_listener
            << " which is "
            << (result > 0 ? "greater" : "lesser")
            << " than expected";
        return false;
    }

    return true;
}

MATCHER_P(HasSchema, expectedSchema, "")
{
    auto schema = GetTableSchemaFromDataSplit(arg);

    if (schema != expectedSchema) {
        *result_listener
            << "actual schema is " << ToString(schema) << " while "
            << "expected schema is " << ToString(expectedSchema);
        return false;
    }

    return true;
}

TKeyColumns GetSampleKeyColumns();
TKeyColumns GetSampleKeyColumns2();
TTableSchema GetSampleTableSchema();

TDataSplit MakeSimpleSplit(const TYPath& path, ui64 counter = 0);
TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns, ui64 counter = 0);

TFuture<TDataSplit> RaiseTableNotFound(const TYPath& path, TTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
