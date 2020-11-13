#include "key.h"

namespace NYT::NTableClient {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_FATAL below.
static const TLogger Logger("TableClientKey");

////////////////////////////////////////////////////////////////////////////////

TKey::TKey(const TUnversionedValue* begin, int length)
    : Begin_(begin)
    , Length_(length)
{ }

TKey TKey::FromRow(const TUnversionedRow& row, std::optional<int> length)
{
    int keyLength = length.value_or(row.GetCount());
    YT_VERIFY(keyLength <= row.GetCount());

    ValidateValueTypes(row.Begin(), row.Begin() + keyLength);

    return TKey(row.Begin(), keyLength);
}

TKey TKey::FromRowUnchecked(const TUnversionedRow& row, std::optional<int> length)
{
    int keyLength = length.value_or(row.GetCount());
    YT_VERIFY(keyLength <= row.GetCount());

#ifndef NDEBUG
    try {
        ValidateValueTypes(row.Begin(), row.Begin() + keyLength);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key from row");
    }
#endif

    return TKey(row.Begin(), keyLength);
}

TUnversionedOwningRow TKey::AsOwningRow() const
{
    return TUnversionedOwningRow(Begin_, Begin_ + Length_);
}

const TUnversionedValue& TKey::operator[](int index) const
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(index < Length_);

    return Begin_[index];
}

int TKey::GetLength() const
{
    return Length_;
}

const TUnversionedValue* TKey::Begin() const
{
    return Begin_;
}

const TUnversionedValue* TKey::End() const
{
    return Begin_ + Length_;
}

void TKey::ValidateValueTypes(
    const TUnversionedValue* begin,
    const TUnversionedValue* end)
{
    for (auto* valuePtr = begin; valuePtr != end; ++valuePtr) {
        ValidateDataValueType(valuePtr->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TKey& lhs, const TKey& rhs)
{
    return CompareRows(lhs.Begin(), lhs.End(), rhs.Begin(), rhs.End()) == 0;
}

bool operator!=(const TKey& lhs, const TKey& rhs)
{
    return !(lhs == rhs);
}

void FormatValue(TStringBuilderBase* builder, const TKey& key, TStringBuf format)
{
    builder->AppendString(ToString(key));
}

TString ToString(const TKey& key)
{
    return Format("[%v]", JoinToString(key.Begin(), key.End()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
