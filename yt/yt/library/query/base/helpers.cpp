#include "helpers.h"
#include "private.h"
#include "query.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NQueryClient {

using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

#ifdef _asan_enabled_
static const int MinimumStackFreeSpace = 128_KB;
#else
static const int MinimumStackFreeSpace = 16_KB;
#endif

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth()
{
    if (!NConcurrency::CheckFreeStackSpace(MinimumStackFreeSpace)) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::QueryExpressionDepthLimitExceeded,
            "Expression depth causes stack overflow");
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogger MakeQueryLogger(TGuid queryId)
{
    return QueryClientLogger().WithTag("FragmentId: %v", queryId);
}

TLogger MakeQueryLogger(TConstBaseQueryPtr query)
{
    return MakeQueryLogger(query->Id);
}

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv", source)
        << TErrorAttribute("lhs_source", lhsSource)
        << TErrorAttribute("rhs_source", rhsSource)
        << TErrorAttribute("lhs_type", lhsType)
        << TErrorAttribute("rhs_type", rhsType);
}

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const std::string& columnName)
{
    for (int index = 0; index < std::ssize(keyColumns); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
}

TLogicalTypePtr ToQLType(const TLogicalTypePtr& columnType)
{
    if (IsV1Type(columnType)) {
        const auto wireType = GetWireType(columnType);
        return MakeLogicalType(GetLogicalType(wireType), /*required*/ false);
    } else {
        return columnType;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
