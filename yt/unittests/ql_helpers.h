#pragma once

#include "framework.h"

#include "versioned_table_client_ut.h"

#include <ytlib/query_client/public.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/plan_fragment.h>

#include <ytlib/table_client/unversioned_row.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/object_client/helpers.h>

#define _MIN_ "<\"type\"=\"min\">#"
#define _MAX_ "<\"type\"=\"max\">#"
#define _NULL_ "<\"type\"=\"null\">#"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TOwningKey& key, ::std::ostream* os);

void PrintTo(const TUnversionedValue& value, ::std::ostream* os);

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

////////////////////////////////////////////////////////////////////////////////

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os);

static TValue MakeInt64(i64 value)
{
    return MakeUnversionedInt64Value(value);
}

static TValue MakeUint64(i64 value)
{
    return MakeUnversionedUint64Value(value);
}

static TValue MakeBoolean(bool value)
{
    return MakeUnversionedBooleanValue(value);
}

static TValue MakeString(const TStringBuf& value)
{
    return MakeUnversionedStringValue(value);
}

template <class TTypedExpression, class... TArgs>
static TConstExpressionPtr Make(TArgs&&... args)
{
    return New<TTypedExpression>(
        EValueType::TheBottom,
        std::forward<TArgs>(args)...);
}

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
    auto expected = BuildKey(encodedLowerBound);
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
    auto expected = BuildKey(encodedUpperBound);
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
        //*result_listener
        //    << "actual counter id is " << schema << " while "
        //    << "expected counter id is " << expectedSchema;
        return false;
    }

    return true;
}

TKeyColumns GetSampleKeyColumns();

TKeyColumns GetSampleKeyColumns2();

TTableSchema GetSampleTableSchema();

template <class T>
TFuture<T> WrapInFuture(const T& value)
{
    return MakeFuture(TErrorOr<T>(value));
}

TFuture<void> WrapVoidInFuture();

TDataSplit MakeSimpleSplit(const TYPath& path, ui64 counter = 0);

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns, TKeyColumns keyColumns = TKeyColumns());

TFuture<TDataSplit> RaiseTableNotFound(
    const TYPath& path,
    TTimestamp);

template <class TFunctor, class TMatcher>
void EXPECT_THROW_THAT(TFunctor functor, TMatcher matcher)
{
    bool exceptionThrown = false;
    try {
        functor();
    } catch (const std::exception& ex) {
        exceptionThrown = true;
        EXPECT_THAT(ex.what(), matcher);
    }
    EXPECT_TRUE(exceptionThrown);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT