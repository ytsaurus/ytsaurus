#pragma once

#include "fwd.h"

#include "private/row_vtable.h"

#include <util/generic/string.h>

#include <compare>
#include <type_traits>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TRow_>
class TTypeTag
{
public:
    using TRow = TRow_;

public:
    explicit TTypeTag(TString description) noexcept
        : Description_(std::move(description))
    { }

    const TString& GetDescription() const
    {
        return Description_;
    }

    void Save(IOutputStream* output) const
    {
        ::Save(output, Description_);
    }

    void Load(IInputStream* input)
    {
        ::Load(input, Description_);
    }

private:
    TString Description_;

    friend class TDynamicTypeTag;
};

class TDynamicTypeTag
{
public:
    using TKey = std::pair<TString, std::intptr_t>;

    TDynamicTypeTag() = default;

    TDynamicTypeTag(TString description, NPrivate::TRowVtable rowVtable)
        : Description_(std::move(description))
        , RowVtable_(std::move(rowVtable))
    { }

    template <typename TRow>
    TDynamicTypeTag(TTypeTag<TRow> typeTag) // NOLINT(google-explicit-constructor)
        : TDynamicTypeTag(std::move(typeTag.Description_), NRoren::NPrivate::MakeRowVtable<TRow>())
    { }

    TString GetDescription() const
    {
        return Description_;
    }

    const NPrivate::TRowVtable& GetRowVtable() const
    {
        return RowVtable_;
    }

    TKey GetKey() const
    {
        return {Description_, reinterpret_cast<std::intptr_t>(RowVtable_.DefaultConstructor)};
    }

    void Save(IOutputStream* output) const
    {
        ::Save(output, Description_);
        ::Save(output, RowVtable_);
    }

    void Load(IInputStream* input)
    {
        ::Load(input, Description_);
        ::Load(input, RowVtable_);
    }

    operator bool() const {
        return IsDefined(RowVtable_);
    }

private:
    TString Description_ = "<undefined-tag>";
    NPrivate::TRowVtable RowVtable_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

template <>
TString ToString(const NRoren::TDynamicTypeTag& tag);

template <typename T>
TString ToString(const NRoren::TTypeTag<T>& tag)
{
    TStringStream out;
    out << "TTypeTag<" << typeid(T).name() << ">{" << tag.GetDescription() << "}";
    return out.Str();
}
