#include "key.h"

#include "serialize.h"

#include <yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NYTree;
using namespace NYson;

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
    if (!row) {
        return TKey();
    }

    int keyLength = length.value_or(row.GetCount());
    YT_VERIFY(keyLength <= row.GetCount());

    ValidateValueTypes(row.Begin(), row.Begin() + keyLength);

    return TKey(row.Begin(), keyLength);
}

TKey TKey::FromRowUnchecked(const TUnversionedRow& row, std::optional<int> length)
{
    if (!row) {
        return TKey();
    }

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

TKey::operator bool() const
{
    return Begin_ != nullptr;
}

TUnversionedOwningRow TKey::AsOwningRow() const
{
    YT_VERIFY(Begin_);

    return TUnversionedOwningRow(Begin_, Begin_ + Length_);
}

const TUnversionedValue& TKey::operator[](int index) const
{
    YT_VERIFY(Begin_);
    YT_VERIFY(index >= 0);
    YT_VERIFY(index < Length_);

    return Begin_[index];
}

int TKey::GetLength() const
{
    YT_VERIFY(Begin_);
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

void TKey::Persist(const TPersistenceContext& context)
{
    if (context.IsSave()) {
        auto representation = Begin_
            ? SerializeToString(Begin_, Begin_ + Length_)
            : SerializedNullRow;
        NYT::Save(context.SaveContext(), representation);
    } else {
        TUnversionedRow row;
        Load(context.LoadContext(), row);
        if (row) {
            // Row lifetime is ensured by row buffer in load context.
            Begin_ = row.Begin();
            Length_ = row.GetCount();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TKey& lhs, const TKey& rhs)
{
    if (!lhs || !rhs) {
        return static_cast<bool>(lhs) == static_cast<bool>(rhs);
    }

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
    if (key) {
        return Format("[%v]", JoinToString(key.Begin(), key.End()));
    } else {
        return "#";
    }
}

void Serialize(const TKey& key, IYsonConsumer* consumer)
{
    if (key) {
        BuildYsonFluently(consumer)
            .DoListFor(MakeRange(key.Begin(), key.End()), [&](TFluentList fluent, const TUnversionedValue& value) {
                fluent
                    .Item()
                    .Value(value);
            });
    } else {
        BuildYsonFluently(consumer)
            .Entity();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
