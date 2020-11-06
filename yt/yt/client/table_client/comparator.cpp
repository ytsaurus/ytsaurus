#include "comparator.h"

#include "key_bound.h"
#include "serialize.h"

#include <yt/core/logging/log.h>

namespace NYT::NTableClient {

using namespace NLogging;

//! Used only for YT_LOG_FATAL below.
static const TLogger Logger("TableClientComparator");

////////////////////////////////////////////////////////////////////////////////

TComparator::TComparator(std::vector<ESortOrder> sortOrders)
    : SortOrders_(std::move(sortOrders))
{ }

void TComparator::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SortOrders_);
}

int TComparator::GetLength() const
{
    return SortOrders_.size();
}

void TComparator::ValidateKey(const TKey& key) const
{
    YT_LOG_FATAL_IF(
        key.GetCount() != GetLength(),
        "Comparator is used with key of different length (Key: %v, Comparator: %v)",
        key.AsRow(),
        *this);
}

void TComparator::ValidateKeyBound(const TKeyBound& keyBound) const
{
    YT_LOG_FATAL_IF(
        keyBound.Prefix.GetCount() > GetLength(),
        "Comparator is used with longer key bound (KeyBound: %v, Comparator: %v)",
        keyBound,
        *this);
}

bool TComparator::TestKey(const TKey& key, const TKeyBound& keyBound) const
{
    ValidateKey(key);
    ValidateKeyBound(keyBound);

    int comparisonResult = 0;

    for (int index = 0; index < keyBound.Prefix.GetCount(); ++index) {
        const auto& keyValue = key[index];
        const auto& keyBoundValue = keyBound.Prefix[index];
        int valueComparisonResult = CompareRowValues(keyValue, keyBoundValue);
        if (valueComparisonResult != 0) {
            // TODO(max42): if sort order == descending, valueComparisonResult = -valueComparisonResult.
            comparisonResult = valueComparisonResult;
            break;
        }
    }

    if (keyBound.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    // Now:
    // - comparisonResult > 0 means that key is strictly inside ray (i.e. test is positive);
    // - comparisonResult == 0 means that key starts with key bound prefix (i.e. we should consider inclusiveness);
    // - comparisonResult < 0 means that key is strictly outside ray (i.e. test is negative).

    return comparisonResult > 0 || (comparisonResult == 0 && keyBound.IsInclusive);
}

void FormatValue(TStringBuilderBase* builder, const TComparator& comparator, TStringBuf /* spec */)
{
    builder->AppendFormat("{Length: %v}", comparator.GetLength());
}

TString ToString(const TComparator& comparator)
{
    return ToStringViaBuilder(comparator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
