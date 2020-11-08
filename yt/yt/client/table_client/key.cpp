#include "key.h"

namespace NYT::NTableClient {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_FATAL below.
static const TLogger Logger("TableClientKey");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
TKeyImpl<TRow> TKeyImpl<TRow>::FromRow(const TRow& row)
{
    ValidateValueTypes(row);
    TKeyImpl<TRow> result;
    static_cast<TRow&>(result) = row;
    return result;
}

template <class TRow>
TKeyImpl<TRow> TKeyImpl<TRow>::FromRow(TRow&& row)
{
    ValidateValueTypes(row);
    TKeyImpl<TRow> result;
    static_cast<TRow&>(result) = row;
    return result;
}

template <class TRow>
TKeyImpl<TRow> TKeyImpl<TRow>::FromRowUnchecked(const TRow& row)
{
#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key from row");
    }
#endif

    TKeyImpl<TRow> result;
    static_cast<TRow&>(result) = row;
    return result;
}

template <class TRow>
TKeyImpl<TRow> TKeyImpl<TRow>::FromRowUnchecked(TRow&& row)
{
#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key from row");
    }
#endif

    TKeyImpl<TRow> result;
    static_cast<TRow&>(result) = row;
    return result;
}

template <class TRow>
void TKeyImpl<TRow>::ValidateValueTypes(const TRow& row)
{
    for (const auto& value : row) {
        ValidateDataValueType(value.Type);
    }
}

template <class TRow>
const TRow& TKeyImpl<TRow>::AsRow() const
{
    return static_cast<const TRow&>(*this);
}

////////////////////////////////////////////////////////////////////////////////

template class TKeyImpl<TUnversionedRow>;
template class TKeyImpl<TUnversionedOwningRow>;

////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////

TKey AsNonOwningKey(const TOwningKey& owningKey)
{
    return TKey::FromRowUnchecked(owningKey.AsRow());
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TKey& key, TStringBuf format)
{
    builder->AppendString(ToString(key));
}

void FormatValue(TStringBuilderBase* builder, const TOwningKey& key, TStringBuf format)
{
    builder->AppendString(ToString(key));
}

TString ToString(const TKey& key)
{
    return ToStringViaBuilder(key);
}

TString ToString(const TOwningKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

} // namespace NYT::NTableClient
