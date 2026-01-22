#include "position_independent_value.h"

#include "position_independent_value_transfer.h"

#ifndef YT_COMPILING_UDF

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/string/format.h>

#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFingerprint GetFarmFingerprint(const TPIValue& value)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    return GetFarmFingerprint(asUnversioned);
}

TFingerprint GetFarmFingerprint(const TPIValue* begin, const TPIValue* end)
{
    auto asUnversionedRange = BorrowFromPI(TPIValueRange(begin, static_cast<size_t>(end - begin)));
    return GetFarmFingerprint(NTableClient::TUnversionedValueRange(
        asUnversionedRange.Begin(),
        asUnversionedRange.Size()));
}

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TPIValue& value, ::std::ostream* os)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    *os << ToString(asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

std::string ToString(const TPIValue& value, bool valueOnly)
{
    TUnversionedValue asUnversioned{};
    MakeUnversionedFromPositionIndependent(&asUnversioned, value);
    return ToString(asUnversioned, valueOnly);
}

////////////////////////////////////////////////////////////////////////////////

static_assert(sizeof(TUnversionedValue) == sizeof(TPIValue), "Structs must have equal size");

////////////////////////////////////////////////////////////////////////////////

int CompareRowValues(const TPIValue& lhs, const TPIValue& rhs)
{
    TUnversionedValue lhsAsUnversioned{};
    MakeUnversionedFromPositionIndependent(&lhsAsUnversioned, lhs);

    TUnversionedValue rhsAsUnversioned{};
    MakeUnversionedFromPositionIndependent(&rhsAsUnversioned, rhs);

    return CompareRowValues(lhsAsUnversioned, rhsAsUnversioned);
}

int CompareRows(const TPIValue* lhsBegin, const TPIValue* lhsEnd, const TPIValue* rhsBegin, const TPIValue* rhsEnd)
{
    auto* lhsCurrent = lhsBegin;
    auto* rhsCurrent = rhsBegin;
    while (lhsCurrent != lhsEnd && rhsCurrent != rhsEnd) {
        int result = CompareRowValues(*lhsCurrent++, *rhsCurrent++);
        if (result != 0) {
            return result;
        }
    }
    return static_cast<int>(lhsEnd - lhsBegin) - static_cast<int>(rhsEnd - rhsBegin);
}

////////////////////////////////////////////////////////////////////////////////

void ToAny(TPIValue* result, TPIValue* value, TExpressionContext* context)
{
    auto unversionedResult = BorrowFromPI(result);
    auto unversionedValue = BorrowFromPI(value);

    // NB: TRowBuffer should be used with caution while executing via WebAssembly engine.
    *unversionedResult.GetValue() = EncodeUnversionedAnyValue(
        *unversionedValue.GetValue(),
        context->GetRowBuffer()->GetPool());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
